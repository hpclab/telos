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

package it.unipi.telos.protocols

import it.unipi.telos.core._
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import scala.collection.mutable
import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import it.unipi.telos.util.TelosPropertiesImmutable
import it.unipi.telos.core.GraphVertexContext
import it.unipi.telos.core.ProtocolVertexContext
import it.unipi.telos.core.RequestVertexContextMessage
import it.unipi.telos.core.ResponseVertexContextMessage
import it.unipi.telos.util.Random

/**
 * Override SparkContext implicits
 * Comment/Uncomment to deactivate completely the Misfire MODE !!!!!
 */

//import it.unipi.thesis.andrea.esposito.spark.MisfireSparkContext._

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 09/04/2014.
 *
 * Naive Protocol which calculates the fragments in a graph (relying that each vertex knows to which partition it belongs to)
 */
class FindSubPartitionProtocol(random_seed: Long = 2014) extends Protocol {

  private val random = new Random(random_seed)

  override def afterSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  override def beforeSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  override def brutalStoppable(): Boolean = false

  override def compute[K: ClassTag](self: ProtocolVertexContext[K],
                                    messages: Seq[Message[K]],
                                    responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                                    aggregator: Option[aggregatorType],
                                    superstep: Int)
  : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {
    val findPartitionContext = self.asInstanceOf[FindSubPartitionProtocolVertexContext[K]]
    val oldValue = findPartitionContext.value
    val oldStrenght = findPartitionContext.strenght
    val newMsgs = new mutable.ArrayBuffer[Message[K]]()
    var noActivity = false

    if (superstep == startStep) {
      messages.foreach(msg => msg match {
        case bootMsg: FindSubPartitionBoostrapMessage[_] =>
          if (findPartitionContext.color != bootMsg.color)
            findPartitionContext.links = findPartitionContext.links.filterNot(link => link == bootMsg.sourceId)
        case _ => throw new Exception("NO Bootstrap Messages?!?")
      })
      newMsgs ++= findPartitionContext.links.map(link => new FindSubPartitionMessage[K](findPartitionContext.getId, name, link, findPartitionContext.value, findPartitionContext.strenght))
    } else {
      if (!messages.isEmpty) {
        val best = messages.map(m => m.asInstanceOf[FindSubPartitionMessage[K]]).max(Ordering.fromLessThan[FindSubPartitionMessage[K]]((a, b) => a.strenght < b.strenght))
        if (oldValue != best.value && oldStrenght < best.strenght) {
          findPartitionContext.value = best.value
          val randomStrenght = if (!findPartitionContext.links.isEmpty) random.nextInt(findPartitionContext.links.size) else 0
          findPartitionContext.strenght = best.strenght + findPartitionContext.links.size + randomStrenght
        }
      }

      if (oldValue != findPartitionContext.value)
        newMsgs ++= findPartitionContext.links.map(link => new FindSubPartitionMessage[K](findPartitionContext.getId, name, link, findPartitionContext.value, findPartitionContext.strenght))

      noActivity = true
    }

    (noActivity, newMsgs.toArray[Message[K]], new Array[RequestVertexContextMessage[K]](0))
  }

  override def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any): (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {
    val findPartitionContext = context.asInstanceOf[FindSubPartitionProtocolVertexContext[K]]
    val msgs = findPartitionContext.links.map(link =>
      new FindSubPartitionBoostrapMessage[K](findPartitionContext.getId(), name, link, findPartitionContext.color))
    (msgs, new Array[RequestVertexContextMessage[K]](0))
  }

  override def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): ProtocolVertexContext[K] = {
    val color = data(0).asInstanceOf[Int]
    val value = id.toString.toLong
    new FindSubPartitionProtocolVertexContext[K](color, value, data.drop(1).map(e => e.asInstanceOf[K]))
  }

  override def step: Int = 1

  override val startStep: Int = 0
  override var name: String = "findpartition"
  override type aggregatorType = Nothing

  /**
   * Initializer of the Protocol.
   * Invoked the first time an [[OnJag]] computation starts.
   * @param sc
   */
  override def init(sc: SparkContext): Unit = ()
}

class FindSubPartitionProtocolVertexContext[K](val color: Int, var value: Long, link_param: Array[K]) extends GraphVertexContext[K] {
  var strenght: Long = link_param.size
  val originalLinksNumber: Int = link_param.length
  override var links: Array[K] = link_param
}

class FindSubPartitionBoostrapMessage[K](val sourceId: K,
                                         val sourceProtocol: String,
                                         val targetId: K,
                                         val color: Int) extends Message[K] {
  val targetProtocol: String = sourceProtocol
}

class FindSubPartitionMessage[K](val sourceId: K,
                                 val sourceProtocol: String,
                                 val targetId: K,
                                 val value: Long,
                                 val strenght: Long) extends Message[K] {
  val targetProtocol: String = sourceProtocol
}

/**
 * Companion Object
 *
 * Commodities functions in order to calculate the Ncut and the NQcut for the balanced k-way mincut
 */
object FindSubPartitionProtocol {

  def computeMincut[K](taggedVertexs: RDD[(K, Vertex[K])]): Double = {
    taggedVertexs.map(e => {
      val findSubPartitionCxt = e._2.protocolsContext(0).asInstanceOf[FindSubPartitionProtocolVertexContext[_]]
      findSubPartitionCxt.originalLinksNumber - findSubPartitionCxt.links.length
    }).reduce(_ + _) / 2.0
  }

  def computeSubPartitionStats[K](taggedVertexs: RDD[(K, Vertex[K])]): RDD[(Int, (Int, Double, Double))] = {
    taggedVertexs.map(e => {
      val findSubPartitionCxt = e._2.protocolsContext(0).asInstanceOf[FindSubPartitionProtocolVertexContext[_]]
      (findSubPartitionCxt.value, findSubPartitionCxt.color)
    }) // Extrapolate foreach vertex the useful data
      .combineByKey[(Int, Long)](
        (color: Int) => (color, 1L),
        (colorC: (Int, Long), color: Int) => (colorC._1, colorC._2 + 1L),
        (colorA: (Int, Long), colorB: (Int, Long)) => (colorA._1, colorA._2 + colorB._2)
      ) // group data in order to create the subPartitions, each entry is: (key, (color, size))
      .keyBy(subPartition => subPartition._2._1) // re-key the entries by color
      .combineByKey[Array[Long]](
        (subPartition: (Long, (Int, Long))) => Array(subPartition._2._2),
        (intermediateResult: Array[Long], subPartition: (Long, (Int, Long))) => intermediateResult :+ subPartition._2._2,
        (intermediateResA: Array[Long], intermediateResB: Array[Long]) => intermediateResA ++ intermediateResB
      ) // combine each subPartition of the same color in order to aggregate the data
      .map(subPartition => {
      val color = subPartition._1
      val n = subPartition._2.length
      val u = subPartition._2.reduce(_ + _).toDouble / subPartition._2.length.toDouble
      val sigma = math.sqrt(subPartition._2.map(size => math.pow(size - u, 2)).reduce(_ + _) / n)
      (color, (n, u, sigma))
    }) // foreach subPartition calcs the size, mean and standard deviation
  }

  def computeAssocValues[K](taggedVertexs: RDD[(K, Vertex[K])]): RDD[(Int, (Long, Long))] = {
    taggedVertexs.map(e => {
      val findSubPartitionCxt = e._2.protocolsContext(0).asInstanceOf[FindSubPartitionProtocolVertexContext[_]]
      (findSubPartitionCxt.value, (findSubPartitionCxt.color, findSubPartitionCxt.links.length, findSubPartitionCxt.originalLinksNumber))
    }) // Extrapolate foreach vertex the useful data
      .combineByKey[(Int, Long, Long)](
        (vertex: (Int, Int, Int)) => (vertex._1, vertex._2.toLong, vertex._3.toLong),
        (intermediateResult: (Int, Long, Long), vertex: (Int, Int, Int)) => (vertex._1, intermediateResult._2 + vertex._2, intermediateResult._3 + vertex._3),
        (intermediateResA: (Int, Long, Long), intermediateResB: (Int, Long, Long)) => (intermediateResA._1, intermediateResA._2 + intermediateResB._2, intermediateResA._3 + intermediateResB._3)
      ) // groupData to create the subPartitions, each entry is: (key, (color, assoc(a,a), assoc(a,V)))
      .keyBy(subPartition => subPartition._2._1) // re-key the entries by color
      .combineByKey[(Long, Long)](
        (subPartition: (Long, (Int, Long, Long))) => (subPartition._2._2, subPartition._2._3),
        (intermediateResult: (Long, Long), subPartition: (Long, (Int, Long, Long))) => (intermediateResult._1 + subPartition._2._2, intermediateResult._2 + subPartition._2._3),
        (intermediateResA: (Long, Long), intermediateResB: (Long, Long)) => (intermediateResA._1 + intermediateResB._1, intermediateResA._2 + intermediateResB._2)
      ) // Sum the similarity of each subPartition in order to have the total assoc
      .mapValues(assoc => (assoc._1 / 2, assoc._2 - assoc._1 / 2)) // Fix for each subPartition the double counting of the direct shared edges
  }
}
