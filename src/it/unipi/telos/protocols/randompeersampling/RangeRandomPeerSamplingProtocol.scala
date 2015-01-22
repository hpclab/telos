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
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import it.unipi.telos.util.TelosPropertiesImmutable
import it.unipi.telos.util.Random


/**
 * Created by Andrea Esposito <and1989@gmail.com> on 17/03/14.
 *
 * Random Peer Sampling service in case of fixed range known IDs.
 */
class RangeRandomPeerSamplingProtocol(startRange: Long,
                                      endRange: Long,
                                      random_seed: Long,
                                      startStep_param: Int = 0,
                                      step_param: Int = 1) extends RandomPeerSamplingProtocol {
  def step: Int = step_param

  val startStep: Int = startStep_param
  type aggregatorType = Nothing

  private val random = new Random(random_seed)

  var startRange_broadcast: Broadcast[Long] = _
  var endRange_broadcast: Broadcast[Long] = _

  /**
   * Initializer of the Protocol.
   * Invoked the first time an [[OnJag]] computation starts.
   * @param sc
   */
  override def init(sc: SparkContext): Unit = {
    startRange_broadcast = sc.broadcast(startRange)
    endRange_broadcast = sc.broadcast(endRange)
  }

  def compute[K: ClassTag](self: ProtocolVertexContext[K],
                           messages: Seq[_ <: Message[K]],
                           responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                           aggregator: Option[RangeRandomPeerSamplingProtocol#aggregatorType],
                           superstep: Int)
  : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]])
  = (false, Array[Message[K]](), Array[RequestVertexContextMessage[K]]())

  def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any)
  : (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]])
  = (Array[Message[K]](), Array[RequestVertexContextMessage[K]]())

  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): ProtocolVertexContext[K] = {
    new RangeRandomPeerSamplingVertexContext[K](startRange_broadcast, endRange_broadcast, random.nextLong())
  }

  def afterSuperstep(sc: SparkContext, superstep: Int,property : TelosPropertiesImmutable): Unit = ()

  def beforeSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  def brutalStoppable(): Boolean = true
}

class RangeRandomPeerSamplingVertexContext[K: ClassTag](val startRange: Broadcast[Long],
                                                        val endRange: Broadcast[Long],
                                                        random_seed: Long) extends RandomPeerSamplingVertexContext[K] {
  private val random = new Random(random_seed)

  def getPeer(): Option[K] = {
    this.synchronized {
      val value = (random.nextDouble() * (endRange.value - startRange.value)).toLong + startRange.value

      assert(value >= startRange.value && value <= endRange.value)

      var ret: Option[K] = None

      val stringClassTag = Manifest.classType[String]("string".getClass)
      implicitly[ClassTag[K]] match {
        case ClassTag.Byte =>
          ret = Some(value.toByte.asInstanceOf[K])
        case ClassTag.Int =>
          ret = Some(value.toInt.asInstanceOf[K])
        case ClassTag.Long =>
          ret = Some(value.toLong.asInstanceOf[K])
        //TODO: Find a way to check also the String's ClassTag
        //case stringClassTag =>
        //  ret = Some(value.toString.asInstanceOf[K])
        case _ => ret = None
      }

      ret
    }
  }


  override def toString(): String = {
    "%s(startRange=%s, endRange=%s)".format(this.getClass.getName, startRange.value, endRange.value)
  }
}
