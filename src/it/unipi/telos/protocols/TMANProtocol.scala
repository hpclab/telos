/**
 * Copyright 2014 Andrea Esposito <and1989@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import scala.collection.mutable
import it.unipi.telos.protocols.randompeersampling.RandomPeerSamplingVertexContext

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 18/10/13.
 *
 * T-MAN Protocol rearranged for the ONJAG platform.
 * Pipeline of random id chucks is employed.
 *
 * Paper:
 * M. Jelasity, A. Montresor, and O. Babaoglu. 2009.
 * "T-Man: Gossip-based fast overlay topology construction".
 * Comput. Netw. 53, 13 (August 2009), 2321-2339.
 */
abstract class TMANProtocol(c: Int, H: Int, R: Int, startStep_param: Int = 0,
                            step_param: Int = 1, name_param: String = "tman") extends Protocol {

  var name: String = name_param
  val startStep: Int = startStep_param
  type aggregatorType = Nothing

  def step: Int = step_param

  override def init(sc: SparkContext): Unit = {

  }

  // override in case of a descriptor that needs a message that is possible to clean from the context references
  def factoryMessage[K, viewType](source: TMANVertexContext[K],
                                  targetId: K,
                                  messageTypeFlag_param: Boolean,
                                  viewDescriptors: viewType): TMANMessage[K] = {
    new TMANMessage[K](source.getId, source.descriptor, targetId, messageTypeFlag_param, viewDescriptors.asInstanceOf[source.viewType])(this)
  }

  def brutalStoppable(): Boolean = true

  def compute[K: ClassTag](self: ProtocolVertexContext[K],
                           messages: Seq[Message[K]],
                           responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                           aggregator: Option[TMANProtocol#aggregatorType],
                           superstep: Int)
  : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) = {
    val tmanContext = self.asInstanceOf[TMANVertexContext[K]]
    val newMsgs = new ListBuffer[Message[K]]()
    val reqMsgs = new ListBuffer[RequestVertexContextMessage[K]]()

    val randomResponseContexts = responseProtocolCxtMsgs.map(m => m.cxt.asInstanceOf[TMANVertexContext[K]])

    val randomTMANWindow = tmanContext.randomIdChunks.sliding(R)

    def prepareBuffer(peerId: K, peerDescriptor: Option[tmanContext.descriptorType]): tmanContext.viewType = {
      var currentRndIdChunk : ListBuffer[K] = null
      if(!randomTMANWindow.isEmpty) currentRndIdChunk = randomTMANWindow.next()
      
      var randomTMANBuffer = randomResponseContexts
      if(currentRndIdChunk!=null)
      {
          randomTMANBuffer = randomResponseContexts.filter(cxt => currentRndIdChunk.exists(id => id == cxt.getId()))
      }
      
      val rndView: tmanContext.viewType = randomTMANBuffer.map(
        peer => (peer.getId, Some(peer.descriptor.asInstanceOf[tmanContext.descriptorType]), 0.asInstanceOf[TMANVertexContext[K]#ageType])
      ).toArray

      // var buffer = tmanContext.merge(tmanContext.view, Array())
      val myselfEntry = (tmanContext.getId, Some(tmanContext.descriptor), 0.asInstanceOf[TMANVertexContext[K]#ageType])
      var buffer = tmanContext.merge(tmanContext.view, rndView.+:(myselfEntry))
      buffer = tmanContext.removeOldest(buffer, H)
      tmanContext.selectView(c, buffer, peerId, peerDescriptor)
    }

    // Active/Push "Thread"
    val p = tmanContext.selectPeer()
    val pushBuffer = prepareBuffer(p._1, p._2)

    //Process pendingPull
    tmanContext.pendingPull.foreach {
      elem =>
        val peerId = elem._1
        val peerDescriptor = elem._2
        val pullBuffer = prepareBuffer(peerId, Some(peerDescriptor))
        newMsgs += factoryMessage(tmanContext, peerId, true, pullBuffer)
    }
    tmanContext.pendingPull.clear()
    tmanContext.randomIdChunks.clear()

    reqMsgs ++= askNextRandomPeerChunks(tmanContext)
    newMsgs += factoryMessage(tmanContext, p._1, false, pushBuffer)

    var globalBuffer: tmanContext.viewType = tmanContext.view.clone()

    // Receive either pull and active messages
    messages.foreach {
      m =>
        val mTMAN = m.asInstanceOf[TMANMessage[K]]
        // push receive
        if (mTMAN.messageTypeFlag) {
          globalBuffer = tmanContext.merge(globalBuffer, mTMAN.descriptors.asInstanceOf[tmanContext.viewType])
        } else {
          // pull receive
          globalBuffer = tmanContext.merge(globalBuffer, mTMAN.descriptors.asInstanceOf[tmanContext.viewType])
          reqMsgs ++= askNextRandomPeerChunks(tmanContext)
          tmanContext.pendingPull.append((mTMAN.sourceId, mTMAN.sourceDescriptor.asInstanceOf[tmanContext.descriptorType]))
        }
    }

    tmanContext.view = tmanContext.selectView(c, globalBuffer)
    tmanContext.increaseAge()

    (true, newMsgs, reqMsgs)
  }

  private def askNextRandomPeerChunks[K: ClassTag](tmanContext: TMANVertexContext[K]): Array[RequestVertexContextMessage[K]] = {
    val msgs = new ListBuffer[RequestVertexContextMessage[K]]()
    // Ask for the next random peers chunk
    val peerSamplingContext = tmanContext.accessProtocol("peersampling")
    peerSamplingContext match {
      case Some(peerCxt) =>
        val rndPeerCxt = peerCxt.asInstanceOf[RandomPeerSamplingVertexContext[K]]
        for (i <- 1 to R) {
          val rndPeer = rndPeerCxt.getPeer()
          rndPeer match {
            case Some(peer) =>
              tmanContext.randomIdChunks += peer
              msgs += new RequestVertexContextMessage[K](tmanContext.getId, peer, name)
            case None => {}
          }
        }
      case None => {}
    }
    msgs.toArray
  }

  def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any)
  : (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]]) =
    (new Array[Message[K]](0), askNextRandomPeerChunks(context.asInstanceOf[TMANVertexContext[K]]))

  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): TMANVertexContext[K]
}

abstract class TMANVertexContext[K] extends ProtocolVertexContext[K] {

  /**
   * Type parameter in order to specialize the age field
   */
  type ageType = Int

  type descriptorType
  /**
   * Descriptor is optional because usually at the beginning each peer doesn't know the descriptor value of the others
   */
  type entryType = (K, Option[_ <: descriptorType], ageType)
  type viewType = Array[entryType]

  var view: viewType
  var descriptor: descriptorType

  var pendingPull = new ListBuffer[(K, descriptorType)]
  var randomIdChunks = new ListBuffer[K]

  override def fixSendable[F](p: ProtocolVertexContext[F]): ProtocolVertexContext[F] = {
    val tmanContext = super.fixSendable(p).asInstanceOf[TMANVertexContext[F]]
    tmanContext.pendingPull = null
    tmanContext.randomIdChunks = null
    tmanContext.view = null
    tmanContext
  }

  override def toString(): String = {
    val strBuilder = new StringBuilder()
    strBuilder.append("[ID: %s, Descriptor: %s, View:".format(getId, descriptor))
    if (view != null) {
      view.foreach(v => strBuilder.append(" (%s, %s, %d)".format(v._1, v._2, v._3)))
    }
    strBuilder.append("]\n")
    strBuilder.toString()
  }

  def removeOldest(view: viewType, H: Int): viewType = {
    val ret = new mutable.UnrolledBuffer[entryType]()
    ret ++= view
    for (i <- 0 until H if ret.length > 0) {
      val older = ret.max(Ordering.fromLessThan[entryType]((a, b) => a._3 < b._3))
      ret -= older
    }
    ret.toArray
  }

  /**
   * The TMAN Ranking Function
   * @param d1 V Descriptor
   * @param d2 Another Descriptor
   * @return rank between d1 and d2
   */
  def rankingFunction(d1: descriptorType, d2: descriptorType): Double

  def selectView(c: Int, buffer: viewType): viewType = selectView(c, buffer, getId, Some(descriptor))

  def selectView(c: Int, buffer: viewType, otherId: K, otherPeerDescriptor: Option[descriptorType]): viewType = {
    val noDuplicates = merge(buffer.clone, new viewType(0)).filterNot(s => s._1 == otherId) // Remove duplicates and other peer entry, maintain the freshest ones
    val size = if (c > noDuplicates.length) noDuplicates.length else c
    val sorted = if (otherPeerDescriptor.isDefined)
      noDuplicates.sortWith((a, b) => {
        if (a._2.isDefined && b._2.isDefined)
          rankingFunction(otherPeerDescriptor.get, a._2.get) < rankingFunction(otherPeerDescriptor.get, b._2.get)
        else
          a._2.isDefined
      })
    else
      noDuplicates
    sorted.slice(0, size)
  }

  def selectPeer(): (K, Option[descriptorType]) = {
    // Procedural Style (fast)
    var minPeer: (K, Double, Option[descriptorType]) = null
    for (i <- 0 until view.length) {
      val v = view(i)
      if (minPeer == null) {
        var distance = Double.MaxValue
        var descriptorPeer: Option[descriptorType] = None
        if (view(i)._2.isDefined) {
          distance = rankingFunction(descriptor, v._2.get)
          descriptorPeer = v._2
        }
        minPeer = (v._1, distance, descriptorPeer)
      } else if (v._2.isDefined) {
        val distance = rankingFunction(descriptor, v._2.get)
        val descriptorPeer: Option[descriptorType] = v._2
        if (distance < minPeer._2)
          minPeer = (v._1, distance, descriptorPeer)
      }
    }
    (minPeer._1, minPeer._3)

    // Function style (slow)
    /*
     val distances = view.map {
       v =>
         if (v._2.isDefined) {
           (v._1, rankingFunction(descriptor, v._2.get), v._2)
         } else {
           (v._1, Double.MaxValue, None)
         }
     }

     val ret = distances.min(Ordering.fromLessThan[(K, Double, Option[descriptorType])]((a, b) => a._2 < b._2))
     (ret._1, ret._3)
     */
  }

  def merge(view1: viewType, view2: viewType): viewType = {

    // More procedural way but also more efficient than functional expression
    val result = new mutable.LinkedHashMap[K, entryType]()
    view1.foreach(e =>
      result.get(e._1) match {
        case Some(entry) =>
          if (entry._2.isDefined && e._3 < entry._3) {
            result.update(e._1, e)
          } else if (e._2.isDefined) {
            result.update(e._1, e)
          }
        case None => result.update(e._1, e)
      }
    )
    view2.foreach(e =>
      result.get(e._1) match {
        case Some(entry) =>
          if (entry._2.isDefined && e._3 < entry._3) {
            result.update(e._1, e)
          } else if (e._2.isDefined) {
            result.update(e._1, e)
          }
        case None => result.update(e._1, e)
      }
    )
    result.values.toArray[entryType]

    // Functional expression
    /*
    (view1 ++ view2).groupBy(v => v._1).map(
      e => {
        e._2.min(Ordering.fromLessThan[entryType](
          (a, b) =>
            if (a._2.isDefined && b._2.isDefined) a._3 < b._3
            else a._2.isDefined))
      }
    ).toArray
    */
  }

  def increaseAge(): Unit = {
    view = view.map(v => (v._1, v._2, (v._3 + 1).asInstanceOf[ageType]))
  }
}

class TMANMessage[K](sourceId: K,
                     val sourceDescriptor: TMANVertexContext[K]#descriptorType,
                     targetId: K,
                     val messageTypeFlag: Boolean,
                     viewDescriptors: TMANVertexContext[K]#viewType)(implicit tmanProtocol: TMANProtocol)
  extends GraphMessage[K, (K, _ <: TMANVertexContext[K]#descriptorType, TMANVertexContext[K]#ageType)](
    sourceId,
    tmanProtocol.name,
    targetId,
    tmanProtocol.name,
    viewDescriptors.asInstanceOf[Array[(K, _ <: TMANVertexContext[K]#descriptorType, TMANVertexContext[K]#ageType)]]) {}

