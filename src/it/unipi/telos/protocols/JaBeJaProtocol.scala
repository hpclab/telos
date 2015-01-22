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

import java.io.FileWriter

import scala.Array.canBuildFrom
import scala.Array.fallbackCanBuildFrom
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.Accumulator
import org.apache.spark.AccumulatorParam
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.DoubleAccumulatorParam
import org.apache.spark.SparkContext.IntAccumulatorParam
import org.apache.spark.SparkContext.LongAccumulatorParam

import it.unipi.telos.core.Message
import it.unipi.telos.core.Protocol
import it.unipi.telos.core.ProtocolVertexContext
import it.unipi.telos.core.RequestVertexContextMessage
import it.unipi.telos.core.ResponseVertexContextMessage
import it.unipi.telos.protocols.randompeersampling.RandomPeerSamplingVertexContext
import it.unipi.telos.util.Random
import it.unipi.telos.util.TelosPropertiesImmutable

object JaBeJaProtocol {
  private[JaBeJaProtocol] implicit val accumSeq = new AccumulatorParam[Seq[(String, String)]] {
    override def addInPlace(r1: Seq[(String, String)], r2: Seq[(String, String)]): Seq[(String, String)] = r1 ++ r2

    override def zero(initialValue: Seq[(String, String)]): Seq[(String, String)] = initialValue
  }

  private[JaBeJaProtocol] implicit val accumSet = new AccumulatorParam[Set[Int]] {
    override def addInPlace(r1: Set[Int], r2: Set[Int]): Set[Int] = (r1 ++ r2)

    override def zero(initialValue: Set[Int]): Set[Int] = initialValue
  }
}

/**
 * Protocol which implements the JA-BE-JA algorithm
 *
 * Paper:
 * Rahimian, F.; Payberah, A.H.; Girdzijauskas, S.; Jelasity, M.; Haridi, S.,
 * "JA-BE-JA: A Distributed Algorithm for Balanced Graph Partitioning,"
 * Self-Adaptive and Self-Organizing Systems (SASO),
 * 2013 IEEE 7th International Conference on , vol., no., pp.51,60, 9-13 Sept. 2013
 *
 * The arrangement of JA-BE-JA to BSP/Pregel-like systems such as Spark are partially described in:
 * Emanuele	Carlini; Patrizio	Dazzi; Andrea	Esposito; Alessandro	Lulli; Laura	Ricci,
 * "Balanced Graph Partitioning with Apache Spark",
 * BigDataCloud 2014 - Third Workshop on Big Data Management in Clouds
 *
 * Extended experiments and further details in:
 * Andrea, Esposito (2014),
 * "ONJAG, network overlays supporting distributed graph processing",
 * M.Sc. Thesis,
 * University of Pisa: Pisa.
 *
 * @param random_seed
 * @param alfa_param
 * @param TStart
 * @param delta
 * @param pickPartnerProbabilities_param
 * @param partnerAttempts_param
 * @param minCutSamplingRate_param    //How many times the protocol has to run before the mincut sampling takes place
 * @param approximate_param
 * @param startStep_param
 * @param step_param
 */
class JaBeJaProtocol(random_seed: Long,
                     alfa_param: Double = 2.0,
                     val TStart: Double = 2.0,
                     delta: Double = 0.003,
                     randomCacheViewSize_param: Int,
                     tmanCacheViewSize_param: Int,
                     partnerPickOrder_param: PartnerPickOrder.Value = PartnerPickOrder.NEIGH_TMAN_RAND,
                     pickPartnerProbabilities_param: Array[Double] = Array(1.0),
                     partnerAttempts_param: Int = 1,
                     minCutSamplingRate_param: Int = 6, // Default set to 2 hyphotetical ``cycles'': swap->receive swap/send answer->receive ack/nack
                     approximate_param: Boolean = true,
                     startStep_param: Int = 0,
                     step_param: Int = 1) extends Protocol with Logging {
  require(minCutSamplingRate_param > 0)
  require(!pickPartnerProbabilities_param.exists(prob => prob < 0) && pickPartnerProbabilities_param.reduce(_ + _) == 1.0)

  /**
   * General Protocol attributes
   */
  var name: String = "jabeja"
  val startStep: Int = startStep_param
  type aggregatorType = Nothing

  def step: Int = step_param

  def brutalStoppable(): Boolean = false

  /**
   * JaBeJa setting attributes
   */
  private var Tr: Double = _
  private var alfa: Double = _
  private var partnerAttempts: Int = _
  private var partnerPickOrder: PartnerPickOrder.Value = _
  private var pickPartnerProbabilities: Array[Double] = _
  private var approximate: Boolean = _
  private var randomCacheViewSize: Int = _
  private var tmanCacheViewSize: Int = _

  private val random = new Random(random_seed)

  /**
   * MinCut attributes
   */
  private var minCutSamplingRate: Int = _
  var bestMinCutList: Accumulator[Seq[(String, String)]] = _

  var bestMinCutSuperstep: Int = _
  var stableMinCut: Boolean = _
  private var counterMinCutSampling: Int = _
  private val BARRIER_MINCUT_STEP = 1

  var bestMinCutValue: Double = _
  private var minCut: Accumulator[Double] = _
  private var unstableMincut: Accumulator[Double] = _

  val mincutHistory: ListBuffer[Double] = new ListBuffer[Double]()

  val DECISIONAL_WINDOW_MINSIZE = 3
  val DECISIONAL_SLICE_SIZE = 3
  val DECISIONAL_THRESOLD_PERCENTAGE = 0.01

  /**
   * Statistic attributes
   */
  //TODO: Create a protocol just for the coloring and mincut sampling!
  private var colorSet: Accumulator[Set[Int]] = _
  private var colorCounter: Array[Accumulator[Int]] = _

  private var triedSwapRequest: Accumulator[Int] = _
  private var swapRequestNeighbours: Accumulator[Int] = _
  private var swapRequestRandom: Accumulator[Int] = _
  private var swapRequestTMAN: Accumulator[Int] = _

  private var ackNeighbours: Accumulator[Int] = _
  private var ackRandom: Accumulator[Int] = _
  private var ackTMAN: Accumulator[Int] = _

  private var nackNeighbours: Accumulator[Int] = _
  private var nackRandom: Accumulator[Int] = _
  private var nackTMAN: Accumulator[Int] = _

  private var reqCxtGeneral: Accumulator[Int] = _
  private var reqCxtDuplicated: Accumulator[Int] = _

  private var triedSwapRequest_total: Accumulator[Long] = _
  private var swapRequestNeighbours_total: Accumulator[Long] = _
  private var swapRequestRandom_total: Accumulator[Long] = _
  private var swapRequestTMAN_total: Accumulator[Long] = _
  private var ackNeighbours_total: Accumulator[Long] = _
  private var ackRandom_total: Accumulator[Long] = _
  private var ackTMAN_total: Accumulator[Long] = _
  private var nackNeighbours_total: Accumulator[Long] = _
  private var nackRandom_total: Accumulator[Long] = _
  private var nackTMAN_total: Accumulator[Long] = _
  private var reqCxtGeneral_total: Accumulator[Long] = _
  private var reqCxtDuplicated_total: Accumulator[Long] = _

  override def init(sc: SparkContext): Unit = {
    //Init JaBeJa attributes
    Tr = TStart
    alfa = alfa_param
    partnerAttempts = partnerAttempts_param
    partnerPickOrder = partnerPickOrder_param
    pickPartnerProbabilities = pickPartnerProbabilities_param
    approximate = approximate_param
    randomCacheViewSize = randomCacheViewSize_param
    tmanCacheViewSize = tmanCacheViewSize_param

    //Init Mincut attributes
    minCutSamplingRate = minCutSamplingRate_param
    bestMinCutList = sc.accumulator(Seq[(String, String)]())(JaBeJaProtocol.accumSeq)
    bestMinCutSuperstep = -1
    bestMinCutValue = Double.MaxValue
    stableMinCut = false
    counterMinCutSampling = minCutSamplingRate + BARRIER_MINCUT_STEP

    minCut = sc.accumulator(0.0)
    unstableMincut = sc.accumulator(0.0)

    //Init Statistic attributes
    colorSet = sc.accumulator[Set[Int]](Set[Int]())(JaBeJaProtocol.accumSet)
    colorCounter = new Array[Accumulator[Int]](0).map(x => sc.accumulator(0))

    triedSwapRequest = sc.accumulator(0)
    swapRequestNeighbours = sc.accumulator(0)
    swapRequestRandom = sc.accumulator(0)
    swapRequestTMAN = sc.accumulator(0)

    ackNeighbours = sc.accumulator(0)
    ackRandom = sc.accumulator(0)
    ackTMAN = sc.accumulator(0)

    nackNeighbours = sc.accumulator(0)
    nackRandom = sc.accumulator(0)
    nackTMAN = sc.accumulator(0)

    reqCxtGeneral = sc.accumulator(0)
    reqCxtDuplicated = sc.accumulator(0)

    triedSwapRequest_total = sc.accumulator(0L)
    swapRequestNeighbours_total = sc.accumulator(0L)
    swapRequestRandom_total = sc.accumulator(0L)
    swapRequestTMAN_total = sc.accumulator(0L)
    ackNeighbours_total = sc.accumulator(0L)
    ackRandom_total = sc.accumulator(0L)
    ackTMAN_total = sc.accumulator(0L)
    nackNeighbours_total = sc.accumulator(0L)
    nackRandom_total = sc.accumulator(0L)
    nackTMAN_total = sc.accumulator(0L)
    reqCxtGeneral_total = sc.accumulator(0L)
    reqCxtDuplicated_total = sc.accumulator(0L)

    // Print configuration log
    val strBuilder = new mutable.StringBuilder()
    strBuilder.append("JaBeJaProtocol Configuration:\n")
    strBuilder.append("\t (alfa, %s),\n".format(alfa))
    strBuilder.append("\t (Tstart, %s),\n".format(TStart))
    strBuilder.append("\t (delta, %s),\n".format(delta))
    strBuilder.append("\t (PartnerPickOrder, %s),\n".format(partnerPickOrder))
    strBuilder.append("\t (PartnerPickProbabilities, %s),\n".format(pickPartnerProbabilities.map(p => "{" + p + "}").mkString("_")))
    strBuilder.append("\t (PartnerAttempt, %s),\n".format(partnerAttempts))
    strBuilder.append("\t (MincutRate, %s),\n".format(minCutSamplingRate))
    strBuilder.append("\t (Approximate, %s)\n".format(approximate))
    logDebug(strBuilder.toString())
  }

  private def exchangeColor[K: ClassTag](self: JaBeJaVertexContext[K],
                                         partner: SendableJaBeJaVertexContext[K]): Unit = {
    val temp = partner.color
    partner.color = self.color
    self.color = temp
  }

  private def updateTMANView[K](self: JaBeJaVertexContext[K], other: SendableJaBeJaVertexContext[K]): Unit = {
    self.accessProtocol("jabejatman") match {
      case Some(cxt) =>
        val cxtTMAN = cxt.asInstanceOf[JaBeJaTMANVertexContext[K]]
        cxtTMAN.descriptor = (self.color, self.getDegree(self.color) / self.neighbourView.size.toDouble)
        if (cxtTMAN.view.exists(elem => elem._1 == other.getId)) {
          cxtTMAN.view = cxtTMAN.view.filterNot(elem => elem._1 == other.getId) :+ ((other.getId, Some((other.color, other.getDegree(other.color) / other.neighbourView.size.toDouble)), 0.asInstanceOf[JaBeJaTMANVertexContext[K]#ageType]))
        }
      case None => {}
    }
  }

  def compute[K: ClassTag](self: ProtocolVertexContext[K],
                           messages: Seq[Message[K]],
                           responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                           aggregator: Option[JaBeJaProtocol#aggregatorType],
                           superstep: Int)
  : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {
    val cxtJaBeJa = self.asInstanceOf[JaBeJaVertexContext[K]]
    val newMsgs = new ArrayBuffer[Message[K]]()
    val reqMsgs = new ArrayBuffer[RequestVertexContextMessage[K]]()

    /**
     * Flag to switch between bootstrap logic and normal logic
     */
    val bootstrapPhase = superstep <= startStep + 1 * step

    if (bestMinCutSuperstep == superstep) {
      bestMinCutList += Seq(((cxtJaBeJa.getId.toString, cxtJaBeJa.color.toString))) // Be Aware! Costly Operation if there are many many peers..
    }

    val (answerSwapRequestMsgs, swapRequestMsgs) = messages.partition {
      case msg: JaBeJaMessage[K] if msg.header != Header.SWAP_REQUEST => true
      case msg => false
    }

    /**
     * Assert to be sure about that we receive at MOST an ACK message
     */
    assert(answerSwapRequestMsgs.filter(m => m match {
      case msg: JaBeJaMessage[K] => msg.header == Header.ACK
      case _ => false
    }).size <= 1)

    /**
     * Procedure to process the incoming messages
     * NOTE: In presence of SWAP_REQUEST messages it is supposed to be called with ONLY that kind of messages
     */
    def processMessages(messages: Seq[Message[K]]): Unit = {
      /*
       *  Flag to catch when the color is swapped during a superstep
       *  (ensuring that in case of multiple swap_request incoming messages at MOST one ack is sent as reply)
       */
      var swappedColorFlag = false
      messages.foreach {
        case jabejaMsg: JaBeJaMessage[K] =>
          updateView(cxtJaBeJa, jabejaMsg.sourceCxt) // Update the sender's context into my neighbour view in case it is one of that
          jabejaMsg.header match {
            case Header.ACK =>
              exchangeColor(cxtJaBeJa, jabejaMsg.sourceCxt)
              cxtJaBeJa.sentSwapRequest = false // Not engaged anymore
              cxtJaBeJa.partnerAttempts = partnerAttempts

              // Update Statistics
              cxtJaBeJa.lastSwapRequestType match {
                case SWAP_TYPE.TMAN => ackTMAN += 1
                case SWAP_TYPE.NEIGHBOURS => ackNeighbours += 1
                case SWAP_TYPE.RANDOM => ackRandom += 1
              }
            case Header.NACK =>
              cxtJaBeJa.sentSwapRequest = false // Not engaged anymore

              // Update Statistics
              cxtJaBeJa.lastSwapRequestType match {
                case SWAP_TYPE.TMAN => nackTMAN += 1
                case SWAP_TYPE.NEIGHBOURS => nackNeighbours += 1
                case SWAP_TYPE.RANDOM => nackRandom += 1
              }
            case Header.SWAP_REQUEST =>
              if (!cxtJaBeJa.sentSwapRequest && !swappedColorFlag && cxtJaBeJa.evaluate(jabejaMsg.sourceCxt, alfa, Tr) > 0) {
                newMsgs += new JaBeJaMessage[K](cxtJaBeJa, name, jabejaMsg.sourceId, name, Header.ACK) // ACK MESSAGE
                exchangeColor(cxtJaBeJa, jabejaMsg.sourceCxt)
                swappedColorFlag = true
                cxtJaBeJa.partnerAttempts = partnerAttempts // Also if i received a swap but it is successful i reset the attempts
              } else {
                newMsgs += new JaBeJaMessage[K](cxtJaBeJa, name, jabejaMsg.sourceId, name, Header.NACK) // NACK Message
              }
          }
          // Inform and give the data to my TMAN layer that i received a fresh node (and so its color that maybe i swapped with)
          updateTMANView(cxtJaBeJa, jabejaMsg.sourceCxt)
      }
    }

    if (counterMinCutSampling > BARRIER_MINCUT_STEP) {
      val shuffledSwapRequestMsgs = Random.shuffle(swapRequestMsgs, cxtJaBeJa.random)
      //val shuffledSwapRequestMsgs = swapRequestMsgs
      processMessages(shuffledSwapRequestMsgs)
      processMessages(answerSwapRequestMsgs)
    } else {
      newMsgs ++= swapRequestMsgs // Postpone the swapRequest to the next superstep
      if (counterMinCutSampling == BARRIER_MINCUT_STEP) {
        processMessages(answerSwapRequestMsgs) // process last ack/nack set in order to have a stable colored graph
        val neighboursReqMsgs = refreshMessages(cxtJaBeJa) // now i have a stable graph so now i can update the neighbours of each peer
        reqMsgs ++= neighboursReqMsgs

        // Statistics
        reqCxtGeneral += neighboursReqMsgs.size
      }
    }

    /**
     * Update the view's contexts with the most freshest ones
     */
    updateView(cxtJaBeJa, responseProtocolCxtMsgs, bootstrapPhase)

    /**
     * When the counter reaches 0 means everything is stable and updated and i can move forward to
     * the local min-cut contribution calculus and sum it to the global one
     */
    if (counterMinCutSampling == 0) {
      minCut += cxtJaBeJa.neighbourView.size - cxtJaBeJa.getDegree(cxtJaBeJa.color) // Update MinCut value

      // Statistics
      colorCounter(cxtJaBeJa.color) += 1
    }
    unstableMincut += cxtJaBeJa.neighbourView.size - cxtJaBeJa.getDegree(cxtJaBeJa.color) // Update Unstable MinCut value

    /**
     * Finished the update/received part of the protocol.
     * Now try to find, if there is a spot, a candidate to exchange the color with!
     */
    var partner: K = null.asInstanceOf[K]

    /*
     * Pick a partner only if the protocol is bootstrapped,
     * there isn't global min-cut calculation in progress and also
     * randomly decided if pick or not in order to avoid the 'everybody' sent SwapRequest stuck case
     */
    if (!bootstrapPhase && counterMinCutSampling > BARRIER_MINCUT_STEP && cxtJaBeJa.partnerAttempts >= 0) {
      val trySwapRequest = cxtJaBeJa.random.nextInt() % 2 == 0
      if (trySwapRequest && !cxtJaBeJa.sentSwapRequest) {
        /**
         * Let's try to find a partner!
         */
        val neighbours = () => {
          val neighbourCandidateId = searchNeighbourPartnerCandidate(cxtJaBeJa)
          if (neighbourCandidateId != null) {
            partner = neighbourCandidateId

            // Statistics
            cxtJaBeJa.lastSwapRequestType = SWAP_TYPE.NEIGHBOURS
            swapRequestNeighbours += 1
          }
        }

        val tman = () => {
          // if Cache Mode is active (so the cache isn't empty) let's check if there is a candidate in the current responseMsgs
          if (!cxtJaBeJa.tmanCacheView.isEmpty) {
            val tmanCacheView = responseProtocolCxtMsgs.filter(msg => cxtJaBeJa.tmanCacheView.exists(id => id == msg.sourceId)).map(msg => (msg.sourceId, msg.cxt.asInstanceOf[SendableJaBeJaVertexContext[K]])).toArray
            val candidates = cxtJaBeJa.findPartner(tmanCacheView, alfa, Tr)
            if (!candidates.isEmpty)
              partner = candidates.head.getId()
          } else {
            // Otherwise pick a tman peer id at random
            cxtJaBeJa.accessProtocol("jabejatman") match {
              case Some(found) =>
                val cxtTMAN = found.asInstanceOf[JaBeJaTMANVertexContext[K]]
                if (!cxtTMAN.view.isEmpty) {
                  val tmanIndex = cxtJaBeJa.random.nextInt(cxtTMAN.view.size) // Naive, could be done a clever pick!
                  partner = cxtTMAN.view(tmanIndex)._1
                }
              case None => {}
            }
          }
          if (partner != null) {
            // Statistics
            cxtJaBeJa.lastSwapRequestType = SWAP_TYPE.TMAN
            swapRequestTMAN += 1
          }
        }

        val random = () => {
          // Cache Mode active let's check if there is a candidate in the current responseMsgs
          if (!cxtJaBeJa.randomCacheView.isEmpty) {
            val randomCacheView = responseProtocolCxtMsgs.filter(msg => cxtJaBeJa.randomCacheView.exists(id => id == msg.sourceId)).map(msg => (msg.sourceId, msg.cxt.asInstanceOf[SendableJaBeJaVertexContext[K]])).toArray
            val candidates = cxtJaBeJa.findPartner(randomCacheView, alfa, Tr)
            if (!candidates.isEmpty)
              partner = candidates.head.getId()
          } else {
            // Otherwise pick an id at random
            cxtJaBeJa.accessProtocol("peersampling") match {
              case Some(found) =>
                val cxtPeerSampling = found.asInstanceOf[RandomPeerSamplingVertexContext[K]]
                cxtPeerSampling.getPeer() match {
                  case Some(peerId) =>
                    partner = peerId
                  case None => {}
                }
              case None => {}
            }
          }
          if (partner != null) {
            // Statistics
            cxtJaBeJa.lastSwapRequestType = SWAP_TYPE.RANDOM
            swapRequestRandom += 1
          }
        }

        var pickOrder: Array[() => Unit] = null;
        partnerPickOrder match {
          case PartnerPickOrder.NEIGH_TMAN_RAND => pickOrder = Array(neighbours, tman, random);
          case PartnerPickOrder.TMAN_NEIGH_RAND => pickOrder = Array(tman, neighbours, random);
          case PartnerPickOrder.RAND_TMAN_NEIGH => pickOrder = Array(random, tman, neighbours);
        }
        pickOrder.foreach(pick => if (partner == null) pick())

        reqMsgs ++= nextTmanCacheChunk(cxtJaBeJa)
        reqMsgs ++= nextRandomCacheChunk(cxtJaBeJa)

        // Statistics
        triedSwapRequest += 1
      }

      if (trySwapRequest && !cxtJaBeJa.sentSwapRequest) {
        // Update only in case previously wasn't sent a SWAP Request
        cxtJaBeJa.sentSwapRequest = partner != null
        if (partner != null) {
          newMsgs += new JaBeJaMessage[K](cxtJaBeJa, name, partner, name, Header.SWAP_REQUEST) // SWAP_REQUEST MESSAGE
        }
        cxtJaBeJa.partnerAttempts -= 1
      }
    }

    /**
     * Status variable to try to stop the algorithm.
     *
     * If it hasn't finished all the attempts,
     * make it continue because i want to be 100% sure that i've tried all possibilities (according to the settings)
     */
    val noActivity = false //((!cxtJaBeJa.sentSwapRequest && partner == null) || (randomCacheViewSize == 0 && tmanCacheViewSize == 0)) && cxtJaBeJa.partnerAttempts < 0

    /**
     * Simulate Original JaBeJa. Refresh all the neighbours that are NOT involved in any ''communications''
     */
    if (counterMinCutSampling > BARRIER_MINCUT_STEP && !approximate) {
      reqMsgs ++= refreshMessages(cxtJaBeJa).filterNot(m => newMsgs.exists(mm => mm.targetId == m.targetId))
    }

    /*
     * If i haven't found any partners and no pending communications occur
     * i clear the request messages allowing the global stop condition to verify
     */
    //if (noActivity && !bootstrapPhase && counterMinCutSampling > BARRIER_MINCUT_STEP && (newMsgs.isEmpty)) // || (randomCacheViewSize == 0 && tmanCacheViewSize == 0)))
    //  reqMsgs.clear()

    /*
     * Delete RequestMessage duplicates
     */
    val newReqMsgs = reqMsgs.clone()
    newReqMsgs.foreach {
      reqMsg =>
        if (reqMsgs.exists(msg =>
          msg != reqMsg && msg.targetId == reqMsg.targetId && msg.targetProtocol == reqMsg.targetProtocol)) {
          reqMsgs -= reqMsg

          // Statistics
          reqCxtDuplicated += 1
        }
    }

    /**
     * The protocol should swap its color at most with one other partner at each superstep
     */
    assert(newMsgs.count(
      m => m match {
        case jabejaMsg: JaBeJaMessage[K] if jabejaMsg.header == Header.ACK => true
        case _ => false
      }) <= 1
    )

    if (stableMinCut && counterMinCutSampling == 0) {
      (true, new Array[Message[K]](0), new Array[RequestVertexContextMessage[K]](0))
    } else if (bootstrapPhase) {
      if (superstep < startStep + 1 * step) {
        val neighboursReqMsgs = refreshMessages(cxtJaBeJa)
        reqMsgs ++= neighboursReqMsgs

        // Statistics
        reqCxtGeneral += neighboursReqMsgs.size
      }
      (false, newMsgs, reqMsgs)
    } else if (counterMinCutSampling <= BARRIER_MINCUT_STEP) {
      (false, newMsgs, reqMsgs)
    } else {
      (noActivity, newMsgs, reqMsgs)
    }
  }

  def nextTmanCacheChunk[K](cxtJaBeJa: JaBeJaVertexContext[K]): TraversableOnce[RequestVertexContextMessage[K]] = {
    cxtJaBeJa.tmanCacheView.clear()
    cxtJaBeJa.accessProtocol("jabejatman") match {
      case Some(found) =>
        val cxtTMAN = found.asInstanceOf[JaBeJaTMANVertexContext[K]]
        val newSlice = cxtTMAN.view.map(e => e._1).slice(0, tmanCacheViewSize)
        cxtJaBeJa.tmanCacheView ++= newSlice
        newSlice.map(e => new RequestVertexContextMessage[K](cxtJaBeJa.getId(), e, name))
      case None => Array[RequestVertexContextMessage[K]]()
    }
  }

  def nextRandomCacheChunk[K](cxtJaBeJa: JaBeJaVertexContext[K]): TraversableOnce[RequestVertexContextMessage[K]] = {
    cxtJaBeJa.randomCacheView.clear()
    val buf = new ArrayBuffer[RequestVertexContextMessage[K]]()
    cxtJaBeJa.accessProtocol("peersampling") match {
      case Some(cxt) =>
        val rndCxt = cxt.asInstanceOf[RandomPeerSamplingVertexContext[K]]
        for (i <- 0 until randomCacheViewSize)
          rndCxt.getPeer() match {
            case Some(peerId) =>
              cxtJaBeJa.randomCacheView += peerId
              buf += new RequestVertexContextMessage[K](cxtJaBeJa.getId(), peerId, name)
            case None => {}
          }
      case None => {}
    }
    buf.toTraversable
  }

  /**
   * Create the request message set to fetch the neighbour's contexts
   * @param cxtJaBeJa
   * @tparam K
   * @return
   */
  private def refreshMessages[K: ClassTag](cxtJaBeJa: JaBeJaVertexContext[K])
  : Array[RequestVertexContextMessage[K]] =
    cxtJaBeJa.neighbourView.map(neighbour =>
      new RequestVertexContextMessage[K](cxtJaBeJa.getId, neighbour._1, name)
    )

  /**
   * Search into the neighbours view of the context parameter to find the best candidate peer to swap its color with.
   * @param cxtJaBeJa
   * @tparam K
   * @return the best candidate ID according to the pick probabilities. null, otherwise.
   */
  private def searchNeighbourPartnerCandidate[K](cxtJaBeJa: JaBeJaVertexContext[K]): K = {
    var ret: K = null.asInstanceOf[K]
    val candidates = cxtJaBeJa.findPartner(alfa, Tr, pickPartnerProbabilities.size)
    if (!candidates.isEmpty) {
      var indexCandidate = 0
      var pickProbability = cxtJaBeJa.random.nextDouble()
      while (indexCandidate < candidates.length - 1 && pickProbability > pickPartnerProbabilities(indexCandidate)) {
        pickProbability -= pickPartnerProbabilities(indexCandidate)
        indexCandidate += 1
      }
      ret = candidates(indexCandidate).getId()
    }
    ret
  }

  /**
   * Update the neighbours view of the JaBeJa Context parameter with the Fresh Set.
   *
   * @param cxtJaBeJa
   * @param respMsgs
   * @param bootstrapPhase
   * @tparam K
   */
  private def updateView[K](cxtJaBeJa: JaBeJaVertexContext[K],
                            respMsgs: Seq[ResponseVertexContextMessage[K]],
                            bootstrapPhase: Boolean
                             ): Unit = {
    respMsgs.foreach {
      msg =>
        val otherCxt = msg.cxt.asInstanceOf[SendableJaBeJaVertexContext[K]]
        updateView(cxtJaBeJa, otherCxt)
        if (!bootstrapPhase)
          updateTMANView(cxtJaBeJa, otherCxt)
    }
  }

  /**
   * Update the neighbours view of the JaBeJa Context parameter with the Fresh Context.
   * Also update the TMAN view accordingly.
   *
   * @param cxtJaBeJa
   * @param freshCxt
   * @tparam K
   */
  private def updateView[K](cxtJaBeJa: JaBeJaVertexContext[K],
                            freshCxt: SendableJaBeJaVertexContext[K]): Unit = {

    val indexView = cxtJaBeJa.neighbourView.indexWhere(entry => entry._1 == freshCxt.getId)
    if (indexView >= 0)
      cxtJaBeJa.neighbourView(indexView) = ((freshCxt.getId, freshCxt))
  }

  def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any)
  : (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) =
    (new Array[Message[K]](0), refreshMessages(context.asInstanceOf[JaBeJaVertexContext[K]]))

  /**
   *
   * @param data At Position 0 must be the color, it must be in [0, maxcolor]. From position 1 there are the neighbours id.
   * @tparam K
   * @return
   */
  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): ProtocolVertexContext[K] = {
    //Statistics
    colorSet += Set(data(0).hashCode()) // Add color to the set such a way it' possible to count how many colors occur

    val cxt = new JaBeJaVertexContext[K](data(0).asInstanceOf[Int])
    cxt.neighbourView = data.drop(1).map(id => (id.asInstanceOf[K], null).asInstanceOf[(K, SendableJaBeJaVertexContext[K])])
    cxt.partnerAttempts = partnerAttempts_param // Use the parameter instead of the broadcast because could haven't been instantiated yet
    cxt.random = new Random(random.nextLong())
    cxt
  }

  /**
   * Scale and shift the superstep value according to the actual starting superstep and the mincut sampling rate
   */
  private def formatToJaBeJaSuperstep(superstep: Int): Int = {
    (((superstep - (startStep + 2 * step + BARRIER_MINCUT_STEP)) / step.toDouble) * (minCutSamplingRate.toDouble / (minCutSamplingRate.toDouble + BARRIER_MINCUT_STEP))).toInt
  }

  /**
   * initialize statistics variable and other amenities about the mincut sampling rate
   */
  def beforeSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = {
    if (superstep == startStep)
      colorCounter = new Array[Accumulator[Int]](colorSet.value.size).map(x => sc.accumulator(0))

    if (counterMinCutSampling < 0) {
      minCut = sc.accumulator(0.0)
      colorCounter = new Array[Accumulator[Int]](colorSet.value.size).map(x => sc.accumulator(0))
      counterMinCutSampling = minCutSamplingRate + BARRIER_MINCUT_STEP
    }
    unstableMincut = sc.accumulator(0.0)

    /**
     * Reset statistic variables
     */
    triedSwapRequest = sc.accumulator(0)(IntAccumulatorParam)
    swapRequestNeighbours = sc.accumulator(0)(IntAccumulatorParam)
    swapRequestRandom = sc.accumulator(0)(IntAccumulatorParam)
    swapRequestTMAN = sc.accumulator(0)(IntAccumulatorParam)
    ackNeighbours = sc.accumulator(0)(IntAccumulatorParam)
    ackRandom = sc.accumulator(0)(IntAccumulatorParam)
    ackTMAN = sc.accumulator(0)(IntAccumulatorParam)
    nackNeighbours = sc.accumulator(0)(IntAccumulatorParam)
    nackRandom = sc.accumulator(0)(IntAccumulatorParam)
    nackTMAN = sc.accumulator(0)(IntAccumulatorParam)
    reqCxtGeneral = sc.accumulator(0)(IntAccumulatorParam)
    reqCxtDuplicated = sc.accumulator(0)(IntAccumulatorParam)
  }

  /**
   * Decrease Temperature of the Simulated Annealing Technique and ameneties about mincut logging and statistics variables
   */
  def afterSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = {
    if (superstep > startStep + 1 * step) {
      val previousTr = Tr
      if (counterMinCutSampling > 0 && Tr != 1.0) {
        var newTr = Tr - delta
        if (newTr < 1.0)
          newTr = 1.0
        Tr = newTr
      }
      logInfo("Unstable Mincut: %s".format(0.5 * unstableMincut.value))
      
      logDebug("Supersteps to mincut sampling: %s".format(counterMinCutSampling))
      if (counterMinCutSampling == 0) {
        val currentMinCut = 0.5 * minCut.value

        var list = mincutHistory
        if (list.size >= DECISIONAL_WINDOW_MINSIZE) {
          list = list.takeRight(DECISIONAL_SLICE_SIZE)
          stableMinCut = math.abs(1.0 - math.abs(list.max / list.min)) < DECISIONAL_THRESOLD_PERCENTAGE
        }

        if (list.isEmpty || bestMinCutValue > currentMinCut) {
          bestMinCutValue = currentMinCut
          bestMinCutList = sc.accumulator(Seq[(String, String)]())(JaBeJaProtocol.accumSeq)
          bestMinCutSuperstep = superstep + step
        }

        mincutHistory += currentMinCut
        
        val printFile = new FileWriter( "mincut.txt", true )
        
		printFile.write(superstep+","+currentMinCut+ "\n" )
		
        printFile.close

        val strBuilder = new mutable.StringBuilder()
        strBuilder.append("MinCut Calculation Log\n")
        strBuilder.append("Last mincut: %s\n".format(currentMinCut))
        strBuilder.append("JaBeJa Superstep: %s\n%s\n".format(formatToJaBeJaSuperstep(superstep), this.toString()))
        strBuilder.append("Current Best Configuration\n")
        strBuilder.append(this.bestMinCutList.value.map(t => "(" + t._1 + "," + t._2 + ")\n").mkString + "\n")
        strBuilder.append(colorCounter.foldLeft((-1, ""))((prec, counter) => {
          val index = prec._1 + 1
          (index, prec._2 + "Color %s: %s\n".format(index, counter.value))
        })._2)
        strBuilder.append("Temperature: %s\n".format(previousTr))
        logDebug(strBuilder.toString())
      }
      counterMinCutSampling = counterMinCutSampling - 1
    }

    /**
     * Print Statistics
     */
    val strBuilder = new mutable.StringBuilder()
    strBuilder.append("Statistics Superstep\n")
    strBuilder.append("*** MSG Statistics: (SwapNeigh: %s), (SwapTMAN: %s), (SwapRand: %s)\n".format(swapRequestNeighbours.value, swapRequestTMAN.value, swapRequestRandom.value))
    strBuilder.append("*** MSG Statistics: (AckNeigh: %s), (AckTMAN: %s), (AckRand: %s)\n".format(ackNeighbours.value, ackTMAN.value, ackRandom.value))
    strBuilder.append("*** MSG Statistics: (NackNeigh: %s), (NackTMAN: %s), (NackRand: %s)\n".format(nackNeighbours.value, nackTMAN.value, nackRandom.value))
    strBuilder.append("*** MSG Statistics: (ReqGen: %s), (ReqDup: %s)\n".format(reqCxtGeneral.value, reqCxtDuplicated.value))
    strBuilder.append("*** MSG Statistics Superstep Total: (Swap: %s), (ACK: %s), (NACK: %s), (Req: %s), (Tried SWAP: %s)\n".format(
      swapRequestNeighbours.value + swapRequestRandom.value + swapRequestTMAN.value,
      ackNeighbours.value + ackRandom.value + ackTMAN.value,
      nackNeighbours.value + nackRandom.value + nackTMAN.value,
      reqCxtGeneral.value,
      triedSwapRequest.value
    ))
    logDebug(strBuilder.toString())

    /**
     * Update Total Statistic Variables
     */
    triedSwapRequest_total += triedSwapRequest.value
    swapRequestNeighbours_total += swapRequestNeighbours.value
    swapRequestRandom_total += swapRequestRandom.value
    swapRequestTMAN_total += swapRequestTMAN.value

    ackNeighbours_total += ackNeighbours.value
    ackRandom_total += ackRandom.value
    ackTMAN_total += ackTMAN.value

    nackNeighbours_total += nackNeighbours.value
    nackRandom_total += nackRandom.value
    nackTMAN_total += nackTMAN.value

    reqCxtGeneral_total += reqCxtGeneral.value
    reqCxtDuplicated_total += reqCxtDuplicated.value
  }

  override def toString(): String = {
    val it = mincutHistory.iterator
    val strBuilder = new mutable.StringBuilder()
    var i = 0
    strBuilder.append("MinCut value log\n")
    while (it.hasNext) {
      strBuilder.append("%s %s\n".format(i, it.next()))
      i = i + 1
    }

    strBuilder.append("\n")
    strBuilder.append("$$$ TOTAL Statistics $$$\n")
    strBuilder.append("$*$ MSG Statistics: (SwapNeigh: %s), (SwapTMAN: %s), (SwapRand: %s)\n".format(
      swapRequestNeighbours_total.value, swapRequestTMAN_total.value, swapRequestRandom_total.value))
    strBuilder.append("$*$ MSG Statistics: (AckNeigh: %s), (AckTMAN: %s), (AckRand: %s)\n".format(
      ackNeighbours_total.value, ackTMAN_total.value, ackRandom_total.value))
    strBuilder.append("$*$ MSG Statistics: (NackNeigh: %s), (NackTMAN: %s), (NackRand: %s)\n".format(
      nackNeighbours_total.value, nackTMAN_total.value, nackRandom_total.value))
    strBuilder.append("$*$ MSG Statistics: (ReqGen: %s), (ReqDup: %s)\n".format(reqCxtGeneral_total.value, reqCxtDuplicated_total.value))
    strBuilder.append("$*$ MSG Statistics Total: (Swap: %s), (ACK: %s), (NACK: %s), (Req: %s), (Req Dup: %s), (Tried SWAP: %s)\n".format(
      swapRequestNeighbours_total.value + swapRequestRandom_total.value + swapRequestTMAN_total.value,
      ackNeighbours_total.value + ackRandom_total.value + ackTMAN_total.value,
      nackNeighbours_total.value + nackRandom_total.value + nackTMAN_total.value,
      reqCxtGeneral_total.value,
      reqCxtDuplicated_total.value,
      triedSwapRequest_total.value
    ))

    strBuilder.toString()
  }
}

class JaBeJaVertexContext[K](color_param: Int) extends ProtocolVertexContext[K] {

  /**
   * The color of the peer
   */
  var color: Int = color_param

  /**
   * Neighbours View
   */
  var neighbourView: Array[(K, SendableJaBeJaVertexContext[K])] = _

  /**
   * Caches
   */
  var tmanCacheView: ArrayBuffer[K] = new ArrayBuffer[K]()
  var randomCacheView: ArrayBuffer[K] = new ArrayBuffer[K]()

  /**
   * How many times has the JaBeJa protocol to try to find partners into the View''s''? (Mainly tuning for the Random and TMAN)
   */
  var partnerAttempts: Int = _

  /**
   * State Flag in order to don't be engaged into 2 or more swap request
   */
  var sentSwapRequest: Boolean = false

  /**
   * Random to decide if it's the time to try to find a partner or not
   */
  var random: Random = null

  /**
   * Statistic variable
   */
  var lastSwapRequestType: SWAP_TYPE.Value = _

  /**
   * Clear the context remaining only its data
   * @return
   */
  override def sendable(): SendableJaBeJaVertexContext[K] = {
    val newNeighbourView: Array[(K, SendableJaBeJaVertexContext[K])] = neighbourView.map {
      case v if v._2 == null => v
      case v if v._2 != null =>
        val vDolly = new SendableJaBeJaVertexContext[K](v._2)
        (v._1, v._2.fixSendable(vDolly).asInstanceOf[SendableJaBeJaVertexContext[K]])
    }

    val dolly = new SendableJaBeJaVertexContext[K](this.color, newNeighbourView)
    super.fixSendable(dolly).asInstanceOf[SendableJaBeJaVertexContext[K]]
  }

  override def toString(): String = {
    "%s %s".format(getId, color)
  }

  def getDegree(color: Int): Int = {
    neighbourView.foldLeft(0)((degree, link) => if (link._2.color == color) degree + 1 else degree)
  }

  def findPartner(view: Array[(K, SendableJaBeJaVertexContext[K])], alfa: Double, Tr: Double, nCandidates: Int = 1): Array[SendableJaBeJaVertexContext[K]] = {
    val (highest, bestPartner) = view.foldLeft[(Double, Array[SendableJaBeJaVertexContext[K]])]((0.0, Array())) {
      case (prec, elem) =>
        val q = elem._2
        val highest = prec._1
        val bestPartnerArr = prec._2
        if (this.color != q.color) {
          val newVal = evaluate(q, alfa, Tr, true)
          if (newVal > highest)
            (newVal, bestPartnerArr.+:(q))
          else
            (highest, bestPartnerArr)
        } else
          (highest, bestPartnerArr)
    }
    bestPartner.slice(0, nCandidates)
  }

  def findPartner(alfa: Double, Tr: Double, nCandidates: Int): Array[SendableJaBeJaVertexContext[K]] = {
    findPartner(neighbourView, alfa, Tr, nCandidates)
  }

  def evaluate(q: SendableJaBeJaVertexContext[K], alfa: Double, Tr: Double): Double = evaluate(q, alfa, Tr, neighbourView.exists(link => link._1 == q.getId()))

  def evaluate(q: SendableJaBeJaVertexContext[K], alfa: Double, Tr: Double, isNeighbour: Boolean): Double = {
    if (this.color == q.color) 0

    var actualCoefficient = Tr
    val Dpp = this.getDegree(this.color)
    val Dqq = q.getDegree(q.color)
    var Dpq = this.getDegree(q.color)
    var Dqp = q.getDegree(this.color)

    // Decrease in case it is a neighbour because over-estimation and loops occur otherwise
    if (isNeighbour) {
      Dpq -= 1
      Dqp -= 1
      actualCoefficient = 1 // Simulate Annealing is turned off for neighbours according to the original PeerSim Ja-BE-Ja source code
    }

    val oldVal = math.pow(Dpp, alfa) + math.pow(Dqq, alfa)
    val newVal = math.pow(Dpq, alfa) + math.pow(Dqp, alfa)

    newVal * actualCoefficient - oldVal
  }
}

/**
 * Minimal JaBeJa Context to send it over the ''network'' (as messages)
 * @param color
 * @param neighbourView
 * @tparam K
 */
class SendableJaBeJaVertexContext[K](var color: Int,
                                     var neighbourView: Array[(K, SendableJaBeJaVertexContext[K])])
  extends ProtocolVertexContext[K] {

  def this(cxt: JaBeJaVertexContext[K]) {
    this(cxt.color, null)
  }

  def this(cxt: SendableJaBeJaVertexContext[K]) {
    this(cxt.color, null)
  }

  /**
   * The same as [[JaBeJaVertexContext.getDegree]]
   * @param color
   * @return
   */
  def getDegree(color: Int): Int = {
    neighbourView.foldLeft(0)((degree, link) => if (link._2.color == color) degree + 1 else degree)
  }

  override def toString(): String = {
    "%s %s".format(getId, color)
  }
}

object Header extends Enumeration with Serializable {
  val SWAP_REQUEST, ACK, NACK = Value
}

/**
 * Enumeration tracing request messages for statistic reasons
 */
object SWAP_TYPE extends Enumeration with Serializable {
  val NEIGHBOURS, RANDOM, TMAN = Value
}

object PartnerPickOrder extends Enumeration with Serializable {
  val NEIGH_TMAN_RAND, TMAN_NEIGH_RAND, RAND_TMAN_NEIGH = Value
}

class JaBeJaMessage[K](@transient source: JaBeJaVertexContext[K],
                       val sourceProtocol: String,
                       val targetId: K,
                       val targetProtocol: String,
                       header_param: Header.Value)
  extends Message[K] {

  override val sourceId: K = source.getId

  val header = header_param
  val sourceCxt: SendableJaBeJaVertexContext[K] = source.sendable() // Pack the sendable version!
}
