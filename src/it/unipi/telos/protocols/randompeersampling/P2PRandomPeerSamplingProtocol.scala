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

package it.unipi.telos.protocols.randompeersampling

import it.unipi.telos.core._
import scala.collection.mutable
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import it.unipi.telos.util.TelosPropertiesImmutable
import it.unipi.telos.util.Random

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 17/10/13.
 *
 * Random Peer Sampling Gossip-based
 *
 * Paper:
 * "Gossip-based peer sampling".
 * M. Jelasity, S. Voulgaris, R. Guerraoui, A. Kermarrec, M. van Steen. ACM Trans.
 * Comput. Syst., Volume 25 Issue 3, August 2007 Article No. 8.
 */
class P2PRandomPeerSamplingProtocol(c: Int,
                                    H: Int,
                                    S: Int,
                                    P: Int,
                                    push: Boolean,
                                    pull: Boolean,
                                    selectPeerMode: PeerModeSelection.Value,
                                    random_seed: Long,
                                    startStep_param: Int = 0,
                                    step_param: Int = 1) extends RandomPeerSamplingProtocol {
  val startStep: Int = startStep_param
  type aggregatorType = Nothing

  def step: Int = step_param

  private val random = new Random(random_seed)

  /**
   * Initializer of the Protocol.
   * Invoked the first time an [[OnJag]] computation starts.
   * @param sc
   */
  override def init(sc: SparkContext): Unit = ()

  def brutalStoppable(): Boolean = true

  def compute[K: ClassTag](self: ProtocolVertexContext[K],
                           messages: Seq[Message[K]],
                           responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                           aggregator: Option[P2PRandomPeerSamplingProtocol#aggregatorType],
                           superstep: Int)
  : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {
    val peerSamplingContext = self.asInstanceOf[P2PRandomPeerSamplingVertexContext[K]]
    val newMsgs = new mutable.ListBuffer[Message[K]]()

    def prepareBuffer() = {
      val buffer = mutable.ListBuffer[(K, P2PRandomPeerSamplingVertexContext[K]#ageType)]()
      buffer += ((peerSamplingContext.getId, 0.asInstanceOf[P2PRandomPeerSamplingVertexContext[K]#ageType]))

      peerSamplingContext.viewDescriptors = Random.shuffle(peerSamplingContext.viewDescriptors, peerSamplingContext.random).toArray

      peerSamplingContext.moveOldest(H)
      buffer ++= (peerSamplingContext.viewDescriptors.slice(0, c / 2))
      buffer.map(b => (b._1, b._2))
    }

    for (i <- 1 to P) {
      val p = peerSamplingContext.selectPeer(selectPeerMode)
      if (push) {
        val buffer = prepareBuffer()
        newMsgs += (new P2PRandomPeerSamplingMessage[K](peerSamplingContext.getId, name, p, name, true, buffer.toArray))
      } else {
        newMsgs += (new P2PRandomPeerSamplingMessage[K](peerSamplingContext.getId, name, p, name, true, new Array[(K, P2PRandomPeerSamplingVertexContext[K]#ageType)](0)))
      }
    }

    messages.foreach {
      (m) =>
        val peerMsg = m.asInstanceOf[P2PRandomPeerSamplingMessage[K]]
        if (peerMsg.messageTypeFlag) {
          if (pull) {
            val buffer = prepareBuffer()
            newMsgs += (new P2PRandomPeerSamplingMessage[K](peerSamplingContext.getId, peerMsg.sourceProtocol, peerMsg.sourceId, name, false, buffer.toArray))
          }
          peerSamplingContext.select(c, H, S, peerMsg.descriptors)
        } else {
          if (pull) {
            peerSamplingContext.select(c, H, S, peerMsg.descriptors)
          }
        }
    }

    peerSamplingContext.increaseAge()

    (true, newMsgs, new Array[RequestVertexContextMessage[K]](0))
  }

  def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any)
  : (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]])
  = (new Array[Message[K]](0), new Array[RequestVertexContextMessage[K]](0))

  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): ProtocolVertexContext[K] = {
    val context = new P2PRandomPeerSamplingVertexContext[K](random.nextLong())
    val viewSize = if (data.length <= c) data.length else c
    context.viewDescriptors = new Array[(K, P2PRandomPeerSamplingVertexContext[K]#ageType)](viewSize)
    for (i <- 0 until data.length if i < c) {
      context.viewDescriptors(i) = (data(i).asInstanceOf[K], 0.asInstanceOf[P2PRandomPeerSamplingVertexContext[K]#ageType])
    }
    context
  }

  def afterSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  def beforeSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()
}

class P2PRandomPeerSamplingVertexContext[K](random_seed: Long) extends RandomPeerSamplingVertexContext[K] {

  /**
   * Type parameter in order to specialize the age field
   */
  type ageType = Byte

  var viewDescriptors: Array[(K, ageType)] = _
  var indexQueue: Int = 0
  var random = new Random(random_seed)

  override def toString = {
    val strBuilder = new StringBuilder
    if (viewDescriptors != null)
      for (link <- viewDescriptors) {
        strBuilder.append("%s(%s, %d)\t".format(getId, link._1, link._2))
      }
    else
      strBuilder.append("%s(empty)".format(getId))

    strBuilder.toString
  }

  override def getPeer(): Option[K] = {
    this.synchronized {
      var rndPeer: (K, ageType) = null
      if (indexQueue < viewDescriptors.length) {
        rndPeer = viewDescriptors(indexQueue)
        indexQueue += 1
      } else {
        rndPeer = viewDescriptors((random.nextDouble() * viewDescriptors.length).asInstanceOf[Int])
      }
      if (rndPeer != null)
        Some(rndPeer._1)
      else
        None
    }
  }

  private[randompeersampling] def selectPeer(mode: PeerModeSelection.Value): K = {
    mode match {
      case PeerModeSelection.RAND =>
        viewDescriptors((random.nextDouble() * viewDescriptors.length).asInstanceOf[Int])._1
      case PeerModeSelection.TAIL =>
        viewDescriptors.max(Ordering.fromLessThan[(K, ageType)]((a, b) => a._2 < b._2))._1
    }
  }

  private[randompeersampling] def moveOldest(H: Int) = {
    val mirrorView = new ArrayBuffer[(K, ageType)]()
    mirrorView ++ viewDescriptors
    for (i <- 1 to H if viewDescriptors.length > i) {
      val oldest = mirrorView.max[(K, ageType)](Ordering.fromLessThan[(K, ageType)]((a, b) => a._2 < b._2))
      val index = viewDescriptors.indexOf(oldest)
      val appo = viewDescriptors(viewDescriptors.length - i)
      viewDescriptors(viewDescriptors.length - i) = oldest
      viewDescriptors(index) = appo
      mirrorView -= oldest
    }
  }

  private[randompeersampling] def select(c: Int, H: Int, S: Int, buffer: Array[(K, ageType)]): Unit = {
    val linksBuffer = new mutable.ListBuffer[(K, ageType)]()
    linksBuffer ++= viewDescriptors
    linksBuffer ++= buffer

    // Temp buffer to store duplicate elements
    val dupLinksBuffer = new mutable.ListBuffer[(K, ageType)]()

    // Remove duplicates and myself
    val f: ((K, ageType)) => K = (a) => a._1
    val map = linksBuffer.groupBy[K](f)
    for (m <- map.values) {
      if (m(0)._1 != getId) {
        dupLinksBuffer ++= m - m.min[(K, ageType)](Ordering.fromLessThan[(K, ageType)]((a, b) => a._2 < b._2))
      }
    }
    linksBuffer --= dupLinksBuffer

    // Remove Old Items
    for (i <- 1 to math.min(H, linksBuffer.size - c) if i > 0) {
      val oldest = linksBuffer.max[(K, ageType)](Ordering.fromLessThan[(K, ageType)]((a, b) => a._2 < b._2))
      linksBuffer -= oldest
    }

    // Remove Head items
    var actualS = math.min(S, linksBuffer.size - c)
    actualS = if (actualS > 0) actualS else 0
    linksBuffer --= linksBuffer.take(actualS)

    // Remove at random
    for (i <- 1 to linksBuffer.size - c if i > 0)
      linksBuffer -= linksBuffer((random.nextDouble() * linksBuffer.size).asInstanceOf[Int])

    viewDescriptors = linksBuffer.toArray
    indexQueue = 0
  }

  private[randompeersampling] def increaseAge(): Unit = {
    viewDescriptors = viewDescriptors.map {
      (a) => (a._1, (a._2 + 1).asInstanceOf[ageType])
    }
  }
}

object PeerModeSelection extends Enumeration with Serializable {
  val RAND, TAIL = Value
}

class P2PRandomPeerSamplingMessage[K](
                                       sourceId: K,
                                       sourceProtocol: String,
                                       targetId: K,
                                       targetProtocol: String,
                                       val messageTypeFlag: Boolean,
                                       viewDescriptors: Array[(K, P2PRandomPeerSamplingVertexContext[K]#ageType)])
  extends GraphMessage[K, (K, P2PRandomPeerSamplingVertexContext[K]#ageType)](
    sourceId,
    sourceProtocol,
    targetId,
    targetProtocol,
    viewDescriptors) {}
