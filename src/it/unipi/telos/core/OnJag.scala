/**
 * Copyright 2014 Andrea Esposito <and1989@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.unipi.telos.core

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner
import org.apache.spark.Logging
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import it.unipi.telos.util.TelosPropertiesImmutable

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 22/02/14.
 */

/**
 * Companion object
 */
object OnJag {
  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_ONLY
  val PERSISTENCE_MODE_DEFAULT = PersistenceMode.DELETE_IMMEDIATELY_NOT_SYNCHED
  val ONJAG_SystemContextName = "onjag_sys_cxt";
}

/**
 * Enumeration about how manage the intermediate RDDs
 */
object PersistenceMode extends Enumeration {
  val
  NOT_DELETE,
  DELETE_IMMEDIATELY_SYNCHED,
  DELETE_IMMEDIATELY_NOT_SYNCHED,
  DELETE_SUPERSTEP_SYNCHED,
  DELETE_SUPERSTEP_NOT_SYNCHED
  = Value
}

/**
 * ONJAG Engine
 *
 * @param sc Spark context
 * @param storageLevel Spark's Storage level
 * @param protocols_param Protocols which will be orchestrateds
 */
class OnJag(@transient sc: SparkContext, storageLevel: StorageLevel, protocols_param: Protocol*) extends Logging with Serializable {

  /**
   * The internal structure would be an associative map but
   * it is too costly so 2 parallel array are employed: protocols and indexes
   */

  // The correct way is to broadcast the protocols in order to allow each worker to access it
  private var protocols_broadcast: Array[Broadcast[Protocol]] = protocols_param.toArray.map(p => sc.broadcast(p))

  /**
   * auxiliary convenient method
   */
  private def protocols() = {
    protocols_broadcast
  }

  /**
   * auxiliary convenient method
   */
  private def protocols(index: Int) = {
    protocols_broadcast(index).value
  }

  /**
   * The global Protocol names map in order to
   */
  private val indexes_broadcast: Broadcast[Array[String]] = sc.broadcast(Array[String](OnJag.ONJAG_SystemContextName) ++ protocols.map(p => p.value.name))

  /**
   * Internal counters for the protocol frequencies
   */
  private val stepsCounter: Broadcast[Array[Int]] = sc.broadcast(new Array[Int](protocols.length).map(s => Int.MinValue))
//  private val stepsCounter: Array[Int] = new Array[Int](protocols.length).map(s => Int.MinValue)

  /**
   * Maximum number of iterations
   */
  private var maxStep = -1

  /**
   * List of out-of-dated RDDs
   */
  private val rddBin = new mutable.UnrolledBuffer[RDD[_]]()

  /**
   * Persistence Policy which is employed along the execution
   */
  private var persistenceMode: PersistenceMode.Value = OnJag.PERSISTENCE_MODE_DEFAULT

  {
    /**
     * Default Constructor: initialize the protocols and
     * re-broadcast them in order to maintain aligned the copies over the workers
     */
    //TODO: Merge the 2 broadcast phases (initialization and constructor) of the protocols
    logDebug("Initializing Protocols")
    protocols_broadcast.foreach(p => p.value.init(sc))
    val tmp_protocols_broadcast = protocols_broadcast.map(p => sc.broadcast(p.value)) // Re-broadcast to update broadcast values
    protocols_broadcast.foreach(p => p.unpersist())
    protocols_broadcast = tmp_protocols_broadcast
  }

  def setMaxStep(s: Int): Unit = {
    maxStep = s
  }

  /**
   * Mediator method creating the vertexes for the ONJAG computation
   * @param id_param Vertex's Id
   * @param data Data which are dispatched to the Protocols, according to their positions respectively.
   * @tparam K Id's type
   * @return ONJAG-ready Vertex
   */
  def createVertex[K: ClassTag](id_param: K, data: Array[Array[Any]]): Vertex[K] = {
    require(data.length == protocols.length)
    val arrProts = new Array[ProtocolVertexContext[K]](protocols.length)

    val protocolBuffer = new ArrayBuffer[ProtocolVertexContext[K]]()
    val baseProtocol = new SystemProtocolVertexContext[K](id_param)
    protocolBuffer += baseProtocol
    for (i <- 0 until protocols.length) {
      arrProts(i) = protocols(i).createProtocolVertexContext[K](id_param, data(i))
      protocolBuffer += arrProts(i)
    }

    val vertexProtocols = protocolBuffer.toArray

    baseProtocol.indexes = indexes_broadcast
    baseProtocol.protocols = vertexProtocols
    arrProts.foreach {
      traversableProt =>
        traversableProt.indexes = indexes_broadcast
        traversableProt.protocols = vertexProtocols
    }

    new Vertex(id_param, arrProts)
  }

  /**
   * Mediator method creating the initial set of messages
   *
   * @param vertices ONJAG-ready Vertices
   * @param data Data which are dispatched to the Protocols, according to their positions respectively.
   * @tparam K Id's type
   * @return ONJAG-ready Messages
   */
  def createInitMessages[K: ClassTag](vertices: RDD[(K, Vertex[K])], data: Array[Any])
  : (Array[RDD[(K, Message[K])]], Array[RDD[(K, RequestVertexContextMessage[K])]]) = {
    require(data.length == protocols.length)
    val retMsgs = protocols.map(e => sc.parallelize(Array[(K, Message[K])]()))
    val retReqMsgs = protocols.map(e => sc.parallelize(Array[(K, RequestVertexContextMessage[K])]()))

    for (i <- 0 until protocols.length) {
      val elem = vertices.map {
        v =>
          val cp = v._2.protocolsContext(i)
          protocols(i).createInitMessages(cp, data(i))
      }.persist(storageLevel)
      elem.foreach(x => {}) // Force Evaluation

      // NOTE: another way (maybe more costly): elem.map(e => e._2).flatMap(arr => arr).keyBy(m => m.targetId).persist(storageLevel)
      retReqMsgs(i) = elem.map(e => e._2).flatMap(arr => arr.map(m => (m.targetId, m))).persist(storageLevel)
      val elemMsgs = elem.map(e => e._1).flatMap(arr => arr.map(m => (m.targetId, m))).persist(storageLevel)

      retReqMsgs(i).foreach(x => {}) // Force Evaluation
      elemMsgs.foreach(x => {}) // Force Evaluation

      elem.unpersist(true) // Not under the Persistence Policy. Force removing and wait

      for (j <- 0 until protocols.length) {
        val protocolName = protocols(j).name
        val filteredMsgs = elemMsgs.filter(m => m._2.targetProtocol == protocolName)
        val tmp = (retMsgs(j) ++ filteredMsgs).persist(storageLevel)
        tmp.foreach(x => {}) // Force Evaluation
        retMsgs(j).unpersist(true)
        retMsgs(j) = tmp
      }

      elemMsgs.unpersist(true) // Not under the Persistence Policy. Force removing and wait
    }

    (retMsgs, retReqMsgs)
  }

  /**
   * ONJAG execution main entry point.
   *
   * @param sc Spark context
   * @param vertices ONJAG-ready vertices
   * @param messages ONJAG-ready messages
   * @param combiner List of combiners. One for each Protocol according to their positions
   * @param aggregator List of aggregators. One for each Protocol according to their positions
   * @param checkpointConditions List of checkpoint conditions. One for each Protocol according to their positions
   * @param partitioner Spark's partitioner which defines how split the records
   * @param persistenceMode Persistence Policy that is employed along the execution for the intermediate and out-of-date RDDs
   * @tparam K Id's type
   * @tparam V Vertex's type
   * @tparam A Aggregator's type
   * @return Final ONJAG-ready vertices
   */
  def run[K: ClassTag, V <: Vertex[K] : ClassTag, A: ClassTag](
		  														property : TelosPropertiesImmutable,
		  														sc: SparkContext,
                                                               vertices: RDD[(K, V)],
                                                               messages: (Array[RDD[(K, Message[K])]], Array[RDD[(K, RequestVertexContextMessage[K])]]),
                                                               combiner: Seq[Option[Combiner[K]]],
                                                               aggregator: Seq[Option[Aggregator[K, V, A]]],
                                                               checkpointConditions: Seq[Option[CheckpointCondition]],
                                                               partitioner: Partitioner,
                                                               persistenceMode: PersistenceMode.Value = OnJag.PERSISTENCE_MODE_DEFAULT
                                                                ): RDD[(K, V)] = {
    require(protocols.length == messages._1.length && messages._1.length == messages._2.length && messages._1.length == combiner.length && combiner.length == aggregator.length)

    this.persistenceMode = persistenceMode

    var superstep = 0
    var verts = vertices.partitionBy(partitioner) // Re-partition the vertexes according to the partitioner
    var msgs = (messages._1.map(rdd => rdd.partitionBy(partitioner)), messages._2.map(rdd => rdd.partitionBy(partitioner))) // Re-partition the messages according to the partitioner
    var noActivity = false
//    val pTotalNumMsgs: Array[Long] = protocols.map(p => 1L)
//    val pTotalNumActiveVerts: Array[Long] = protocols.map(p => 1L)

    do {
      logInfo("### Starting superstep " + superstep + ".")
      val startTime = System.currentTimeMillis

      if (superstep <= maxStep) {
        var tmpProtocolBroadcast: Broadcast[Protocol] = null
//        val arrStepsCounter = stepsCounter.value
        val arrStepsCounter = stepsCounter
        val newMsgs = protocols.map(e => sc.parallelize(Array[(K, Message[K])]()))
        val superstep_ = superstep // Create a read-only copy of superstep for capture in closure
        for (i <- 0 until protocols.length) {
          if (superstep >= protocols(i).startStep) {
            val executable = arrStepsCounter.value(i) <= 0
            if (executable) {
              val startTimeProtocol = System.currentTimeMillis

              resetDebugTimer()
              val responseMsgs: RDD[(K, ResponseVertexContextMessage[K])] = compRespMsgs(verts, msgs._2(i))
              printDebug("ResponseMsgs creation")

              val aggregated = agg(verts, aggregator(i))
              val combinedMsgs: RDD[(K, Message[K])] = combiner(i) match {
                case Some(combiner) => msgs._1(i).combineByKey(
                  (m: Any) => combiner.createCombiner(m.asInstanceOf[Message[K]]),
                  (c: Any, m: Any) => combiner.mergeMsg(c.asInstanceOf[Combiner[K]#C], m.asInstanceOf[Message[K]]),
                  (c1: Any, c2: Any) => combiner.mergeCombiners(c1.asInstanceOf[Combiner[K]#C], c2.asInstanceOf[Combiner[K]#C]),
                  partitioner)
                  .flatMap(ext => ext._2.map[(K, Message[K]), TraversableOnce[(K, Message[K])]]((inter: Message[K]) => (ext._1, inter)))
                case None => msgs._1(i)
              }

              resetDebugTimer()
              val grouped = verts.groupWith(combinedMsgs, responseMsgs).persist(storageLevel)
              grouped.foreach(x => {}) // Force Evaluation
              printDebug("Grouped creation")
              
              // Drop old and intermediate RDDs
              persistenceBin(verts)
              persistenceBin(msgs._1(i))
              persistenceBin(msgs._2(i))
              persistenceBin(combinedMsgs)
              persistenceBin(responseMsgs)

              resetDebugTimer()
              // DO actually the Protocol's superstep
              protocols(i).beforeSuperstep(sc, superstep_, property)
              tmpProtocolBroadcast = sc.broadcast(protocols(i)) // Re-broadcast to update broadcast values
              protocols_broadcast(i).unpersist()
              protocols_broadcast(i) = tmpProtocolBroadcast
              val (processed, newProtocolNumMsgs, newProtocolNumActiveVerts) =
                comp(sc, grouped, aggregated, i, superstep_)
              // After comp should not be modified any protocol's ``broadcast'' attributes so no re-broadcast is performed
              protocols(i).afterSuperstep(sc, superstep_, property)
              tmpProtocolBroadcast = sc.broadcast(protocols(i)) // Re-broadcast to update broadcast values
              protocols_broadcast(i).unpersist()
              protocols_broadcast(i) = tmpProtocolBroadcast
              printDebug("Comp function")

              persistenceBin(grouped)

              val timeTaken = System.currentTimeMillis - startTimeProtocol
//              logInfo(
//                "#*#*#*# Protocol %d(%s) Superstep %d took %d s (Active Verts: %s, Outcoming Msgs: %s)".format(
//                  i,
//                  indexes_broadcast.value(i + 1),
//                  superstep, timeTaken / 1000,
//                  newProtocolNumActiveVerts,
//                  newProtocolNumMsgs
//                )
//              )

//              pTotalNumMsgs(i) = newProtocolNumMsgs
//              pTotalNumActiveVerts(i) = newProtocolNumActiveVerts

              resetDebugTimer()
              verts = processed.mapValues(elem => elem._1).persist(storageLevel)
              printDebug("Verts extraction")

              resetDebugTimer()
              msgs._2(i) = processed.flatMap(e => e._2._3.map(m => (m.targetId, m))).persist(storageLevel)
              printDebug("Req Msgs")

              resetDebugTimer()
              val protocolMsgs = processed.flatMap(e => e._2._2.map(m => (m.targetId, m))).persist(storageLevel)
              protocolMsgs.foreach(x => {}) // Force Evaluation

              val tempRdds: Array[RDD[_]] = new Array(protocols.length)
              val oldNewMsgs: Array[RDD[_]] = new Array(protocols.length)
              for (j <- 0 until protocols.length) {
                val protocolName = protocols(j).name
                val filteredProtocolMsgs = protocolMsgs.filter(m => m._2.targetProtocol == protocolName)
                val tmp = (newMsgs(j) ++ filteredProtocolMsgs).persist(storageLevel)
                tempRdds(j) = filteredProtocolMsgs
                oldNewMsgs(j) = newMsgs(j)
                newMsgs(j) = tmp
              }
              printDebug("Protocol Msgs extraction")

              // Mark RDDs as checkpointable BEFORE their materialization so actually it is performed
              val checkpointable = cond(checkpointConditions(i), protocols(i), superstep_)
              if (checkpointable) {
                newMsgs(i).checkpoint() // Just the flag...
                msgs._2(i).checkpoint() // Just the flag...
                verts.checkpoint() // Just the flag...
              }

              // Force Evaluation of the final results
              verts.foreach(x => {}) // Materialize [and do checkpoint]
              msgs._2(i).foreach(x => {}) // Materialize [and do checkpoint]
              newMsgs.foreach(rdd => rdd.foreach(x => {})) // Materialize [and do checkpoint]

              // Drop intermediate RDDs
              tempRdds.foreach(rdd => persistenceBin(rdd))
              oldNewMsgs.foreach(rdd => persistenceBin(rdd))
              persistenceBin(protocolMsgs)
              persistenceBin(processed)

              arrStepsCounter.value(i) = protocols(i).step
            } else {
              if (newMsgs(i).count() > 0) {
                val tmp = (newMsgs(i) ++ msgs._1(i)).persist(storageLevel)
                tmp.foreach(x => {}) // Force Evaluation
                persistenceBin(newMsgs(i))
                persistenceBin(msgs._1(i))
                newMsgs(i) = tmp
              } else {
                newMsgs(i) = msgs._1(i)
              }
            }

            arrStepsCounter.value(i) -= 1
          } else {
            if (newMsgs(i).count() > 0) {
              val tmp = (newMsgs(i) ++ msgs._1(i)).persist(storageLevel)
              tmp.foreach(x => {}) // Force Evaluation
              persistenceBin(newMsgs(i))
              persistenceBin(msgs._1(i))
              newMsgs(i) = tmp
            } else {
              newMsgs(i) = msgs._1(i)
            }
          }
        }

        msgs = (newMsgs, msgs._2)

        // Fix Total Active Verts considering the brutal flag of each protocol
//        for (i <- 0 until protocols.length if protocols(i).brutalStoppable()) {
//          pTotalNumActiveVerts(i) = 0
//        }
//        val vertsWantStop = !pTotalNumActiveVerts.exists(num => num != 0)
//        if (vertsWantStop) {
//          // Clear out-coming [[RequestVertexContextMessage]]
//          for (i <- 0 until protocols.length if protocols(i).brutalStoppable()) {
//            persistenceBin(msgs._2(i))
//            msgs._2(i) = sc.parallelize(Array[(K, RequestVertexContextMessage[K])]()).partitionBy(partitioner)
//          }
//          // Drop the brutal stoppable protocol's [[Message]]s
//          val brutableStoppableProtocolNames = protocols.filter(p => p.value.brutalStoppable()).map(p => p.value.name)
//          for (i <- 0 until protocols.length) {
//            val tmp = msgs._1(i).filter(m => !brutableStoppableProtocolNames.contains(m._2.sourceProtocol)).persist(storageLevel)
//            tmp.foreach(x => {}) // Force Evaluation
//            persistenceBin(msgs._1(i))
//            msgs._1(i) = tmp
//            pTotalNumMsgs(i) = msgs._1(i).count() // Recount how many messages are actually out-coming
//          }
//        }

//        noActivity = !pTotalNumMsgs.exists(num => num != 0) && vertsWantStop
        noActivity = false
        persistenceBinSuperstep() // Drop all the old and intermediate RDDs according to the Persistence Policy

        val timeTaken = System.currentTimeMillis - startTime
        logInfo("### Superstep %d took %d s".format(superstep, timeTaken / 1000))

        superstep += 1
      } else {
        noActivity = true
      }
    } while (!noActivity)

    verts
  }

  def run[K: ClassTag, V <: Vertex[K] : ClassTag, A: ClassTag](
                                                                property : TelosPropertiesImmutable,
		  														sc: SparkContext,
                                                                vertices: RDD[(K, V)],
                                                                messages: (Array[RDD[(K, Message[K])]], Array[RDD[(K, RequestVertexContextMessage[K])]]),
                                                                combiner: Seq[Option[Combiner[K]]],
                                                                aggregator: Seq[Option[Aggregator[K, V, A]]],
                                                                checkpointConditions: Seq[Option[CheckpointCondition]],
                                                                numPartitions: Int,
                                                                persistenceMode: PersistenceMode.Value
                                                                ): RDD[(K, V)] = {
    run(property, sc, vertices, messages, combiner, aggregator, checkpointConditions, new HashPartitioner(numPartitions), persistenceMode)
  }

  def run[K: ClassTag, V <: Vertex[K] : ClassTag, A: ClassTag](
		  														property : TelosPropertiesImmutable,
                                                                sc: SparkContext,
                                                                vertices: RDD[(K, V)],
                                                                messages: (Array[RDD[(K, Message[K])]], Array[RDD[(K, RequestVertexContextMessage[K])]]),
                                                                combiner: Seq[Option[Combiner[K]]],
                                                                aggregator: Seq[Option[Aggregator[K, V, A]]],
                                                                checkpointConditions: Seq[Option[CheckpointCondition]],
                                                                numPartitions: Int
                                                                ): RDD[(K, V)] = {
    run(property, sc, vertices, messages, combiner, aggregator, checkpointConditions, numPartitions, OnJag.PERSISTENCE_MODE_DEFAULT)
  }

  def run[K: ClassTag, V <: Vertex[K] : ClassTag](
                                                   property : TelosPropertiesImmutable,
		  											sc: SparkContext,
                                                   vertices: RDD[(K, V)],
                                                   messages: (Array[RDD[(K, Message[K])]], Array[RDD[(K, RequestVertexContextMessage[K])]]),
                                                   checkpointConditions: Seq[Option[CheckpointCondition]],
                                                   numPartitions: Int,
                                                   persistenceMode: PersistenceMode.Value
                                                   ): RDD[(K, V)] = {
    val combiner: Seq[Option[Combiner[K]]] = protocols.map(p => None)
    val aggregator: Seq[Option[Aggregator[K, V, Any]]] = protocols.map(p => None)
    run(property, sc, vertices, messages, combiner, aggregator, checkpointConditions, numPartitions, persistenceMode)
  }

  def run[K: ClassTag, V <: Vertex[K] : ClassTag](
                                                   property : TelosPropertiesImmutable,
		  											sc: SparkContext,
                                                   vertices: RDD[(K, V)],
                                                   messages: (Array[RDD[(K, Message[K])]], Array[RDD[(K, RequestVertexContextMessage[K])]]),
                                                   checkpointConditions: Seq[Option[CheckpointCondition]],
                                                   numPartitions: Int
                                                   ): RDD[(K, V)] = {
    run(property, sc, vertices, messages, checkpointConditions, numPartitions, OnJag.PERSISTENCE_MODE_DEFAULT)
  }

  def run[K: ClassTag, V <: Vertex[K] : ClassTag](
                                                   property : TelosPropertiesImmutable,
		  											sc: SparkContext,
                                                   vertices: RDD[(K, V)],
                                                   messages: (Array[RDD[(K, Message[K])]], Array[RDD[(K, RequestVertexContextMessage[K])]]),
                                                   numPartitions: Int,
                                                   persistenceMode: PersistenceMode.Value
                                                   ): RDD[(K, V)] = {
    val checkpointConditions = protocols.map(p => None)
    run(property, sc, vertices, messages, checkpointConditions, numPartitions, persistenceMode)
  }

  def run[K: ClassTag, V <: Vertex[K] : ClassTag](
                                                   property : TelosPropertiesImmutable,
		  											sc: SparkContext,
                                                   vertices: RDD[(K, V)],
                                                   messages: (Array[RDD[(K, Message[K])]], Array[RDD[(K, RequestVertexContextMessage[K])]]),
                                                   numPartitions: Int
                                                   ): RDD[(K, V)] = {
    run(property, sc, vertices, messages, numPartitions, OnJag.PERSISTENCE_MODE_DEFAULT)
  }

  /**
   * Debug variable
   */
  private var startDebugTimeProtocol = System.currentTimeMillis

  /**
   * Reset the debug timer
   */
  private def resetDebugTimer() = startDebugTimeProtocol = System.currentTimeMillis

  // Sample performance
  private def printDebug(msg: String) = {
    val debugTimeProtocol = System.currentTimeMillis
    logDebug("%s took %s sec".format(msg, (debugTimeProtocol - startDebugTimeProtocol) / 1000.0))
  }

  /**
   * Perform the dropping of the RDDs according to the Persistence Policy at each protocol's execution
   *
   * @param rdd
   */
  private def persistenceBin(rdd: RDD[_]): Unit = {
    persistenceMode match {
      case PersistenceMode.DELETE_IMMEDIATELY_NOT_SYNCHED => rdd.unpersist(false)
      case PersistenceMode.DELETE_IMMEDIATELY_SYNCHED => rdd.unpersist(true)
      case PersistenceMode.DELETE_SUPERSTEP_NOT_SYNCHED => rddBin += rdd
      case PersistenceMode.DELETE_SUPERSTEP_SYNCHED => rddBin += rdd
      case PersistenceMode.NOT_DELETE => {}
    }
  }

  /**
   * Perform the dropping of the RDDs according to the Persistence Policy at each superstep
   */
  private def persistenceBinSuperstep(): Unit = {
    persistenceMode match {
      case PersistenceMode.DELETE_SUPERSTEP_NOT_SYNCHED => rddBin.foreach(rdd => rdd.unpersist(false))
      case PersistenceMode.DELETE_SUPERSTEP_SYNCHED => rddBin.foreach(rdd => rdd.unpersist(true))
      case PersistenceMode.DELETE_IMMEDIATELY_NOT_SYNCHED => {}
      case PersistenceMode.DELETE_IMMEDIATELY_SYNCHED => {}
      case PersistenceMode.NOT_DELETE => {}
    }
    rddBin.clear()
  }

  /**
   * Aggregates the given vertices using the given aggregator, if it is specified.
   */
  private def agg[K: ClassTag, V <: Vertex[K] : ClassTag, A: ClassTag](
                                                                        verts: RDD[(K, V)],
                                                                        aggregator: Option[Aggregator[K, V, A]]
                                                                        ): Option[A] = {
    aggregator match {
      case Some(a) =>
        Some(verts.map {
          case (id, vert) => a.createAggregator(vert)
        }.reduce(a.mergeAggregators(_, _)))
      case None => None
    }
  }

  /**
   * Executes an actual protocol computation
   */
  private def comp[K: ClassTag, V <: Vertex[K] : ClassTag, A: ClassTag](
                                                                         sc: SparkContext,
                                                                         grouped: RDD[(K, (Iterable[V], Iterable[Message[K]], Iterable[ResponseVertexContextMessage[K]]))],
                                                                         aggregated: Option[A],
                                                                         indexProtocol: Int,
                                                                         superstep: Int
                                                                         ): (RDD[(K, (V, Iterable[Message[K]], Iterable[RequestVertexContextMessage[K]]))], Long, Long) = {

    val comp_numMsgs = sc.accumulator(0L)
    val comp_numActiveVerts = sc.accumulator(0L)

    // protocol must be set inside the flatMap otherwise a copy of it will be used inside!
    val processed = grouped.flatMapValues {
      case (vs, combinedMsgs, reqMsgs) if vs.size == 0 =>
//        throw new IllegalStateException(
//          "Messages to no available vertexes:\n# Combined Msgs:\n%s#Request Msgs:\n%s".format(
//            combinedMsgs.map(reqMsg => "From: %s To: %s".format(reqMsg.sourceId, reqMsg.targetId)).mkString("\t\n"),
//            reqMsgs.map(reqMsg => "From: %s To: %s".format(reqMsg.sourceId, reqMsg.targetId)).mkString("\t\n")
//          )
//        )
    	  Iterator()
      case (vs, combinedMsgs, reqMsgs) =>
        val protocol = protocols(indexProtocol)
        val castedAggregated = aggregated.asInstanceOf[Option[protocol.aggregatorType]]
        val (noActivity, newMsgs, newReqs) = protocol.compute(
          vs.head.protocolsContext(indexProtocol),
          combinedMsgs.toSeq,
          reqMsgs.toSeq,
          castedAggregated,
          superstep)

        comp_numMsgs += newMsgs.size + newReqs.size
        if (!noActivity)
          comp_numActiveVerts += 1

        Some((vs.head, newMsgs.toIterable, newReqs.toIterable))
    }.persist(storageLevel)

    // Force evaluation of processed RDD for accurate performance measurements
    // Force evaluation otherwise CLOUSURE DOESN'T WORK (i.e. accumulators won't accumulate)
    processed.foreach(x => {})

    (processed, comp_numMsgs.value, comp_numActiveVerts.value)
  }

  /**
   * Retrieves and dispatches the requested Vertex's contexts
   */
  private def compRespMsgs[K: ClassTag, V <: Vertex[K] : ClassTag](verts: RDD[(K, V)], reqMsgs: RDD[(K, RequestVertexContextMessage[K])])
  : RDD[(K, ResponseVertexContextMessage[K])] = {
    // Process Request Messages and create Responses using sendable contexts
    val groupedReqs = verts.groupWith(reqMsgs)
    val responseMsgs = groupedReqs.flatMap {
      case (id, (vs, reqMsgs)) if vs.size == 0 =>
//        throw new IllegalStateException(
//          "Request Messages to no available vertexes:\n%s".format(
//            reqMsgs.map(reqMsg => "From: %s To: %s".format(reqMsg.sourceId, reqMsg.targetId)).mkString("\t\n")
//          )
//        )
    	  Iterator()
      case (id, (vs, reqMsgs)) =>
        val reqMap = reqMsgs.groupBy(reqMsg => reqMsg.targetProtocol)
        reqMap.flatMap {
          elem =>
            val (targetProtocol, reqMsgs) = (elem._1, elem._2)
            val indexTargetProtocol = indexes_broadcast.value.indexOf(targetProtocol) - 1
            val sendableCxt = vs.head.protocolsContext(indexTargetProtocol).sendable()
            reqMsgs.map {
              reqMsg =>
                (reqMsg.sourceId, new ResponseVertexContextMessage[K](
                  vs.head.id,
                  targetProtocol,
                  reqMsg.sourceId,
                  sendableCxt
                ))
            }
        }
    }
    responseMsgs
  }

  /**
   * Evaluate if the protocol has to be check-pointed
   */
  private def cond(condition: Option[CheckpointCondition], protocol: Protocol, superstep: Int): Boolean = {
    false
//	  condition match {
//      case Some(condition) =>
//        condition.condition(protocol, superstep)
//      case None => false
//    }
  }
}