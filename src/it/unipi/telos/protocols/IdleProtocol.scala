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
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import it.unipi.telos.util.TelosPropertiesImmutable

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 08/11/13.
 *
 * Idle protocol which is NOT brutal stoppable.
 */
class IdleProtocol extends Protocol {

  type aggregatorType = Nothing
  var name: String = "idle"
  val startStep: Int = 0

  def step: Int = 1

  def brutalStoppable(): Boolean = false

  def afterSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  def beforeSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  def compute[K: ClassTag](self: ProtocolVertexContext[K],
                                                         messages: Seq[_ <: Message[K]],
                                                         responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
                                                         aggregator: Option[IdleProtocol#aggregatorType],
                                                         superstep: Int)
  : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]])
  = (false, new Array[Message[K]](0), new Array[RequestVertexContextMessage[K]](0))

  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): ProtocolVertexContext[K] = new EmptyProtocolVertexContext[K]

  def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any)
  : (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]])
  = (new Array[Message[K]](0), new Array[RequestVertexContextMessage[K]](0))

  /**
   * Initializer of the Protocol.
   * Invoked the first time an [[OnJag]] computation starts.
   * @param sc
   */
  override def init(sc: SparkContext): Unit = ()
}
