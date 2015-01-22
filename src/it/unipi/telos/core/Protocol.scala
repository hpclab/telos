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

import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import it.unipi.telos.util.TelosPropertiesImmutable

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 11/10/13
 *
 * Generic Protocol abstract class
 */
abstract class Protocol extends Serializable {

  type aggregatorType

  /**
   * Name of the protocol, which has to be unique during the execution
   */
  var name: String

  val startStep: Int

  def step: Int // possible to change at runtime

  /**
   * Customizable method for creating ONJAG-ready vertices
   * @param id Vertex's Id
   * @param data User-parameter data
   * @tparam K Id's type
   * @return The vertex context relatives to this protocol
   */
  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): ProtocolVertexContext[K]

  /**
   * Customizable method for creating the ONJAG-ready messages
   * @param context the vertex context of this protocol
   * @param data User-parameter data
   * @tparam K Id's type
   * @return ONJAG-ready messages
   */
  def createInitMessages[K: ClassTag](context: ProtocolVertexContext[K], data: Any)
  : (Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]])

  /**
   * Logic function which is executed in parallel for each vertex
   *
   * @param self the current vertex context that has to be executed
   * @param messages incoming messages
   * @param responseProtocolCxtMsgs response messages
   * @param aggregator aggregator's result
   * @param superstep Current superstep
   * @tparam K Id's type
   * @return Out-coming messages and Request Messages
   */
  def compute[K: ClassTag](self: ProtocolVertexContext[K],
    messages: Seq[Message[K]],
    responseProtocolCxtMsgs: Seq[ResponseVertexContextMessage[K]],
    aggregator: Option[aggregatorType],
    superstep: Int)
    : (Boolean, Seq[_ <: Message[K]], Seq[RequestVertexContextMessage[K]])

  /**
   * Define if this protocol could be stopped ignoring the vertices flags and the messages
   * @return true if brutal stoppable, false otherwise.
   */
  def brutalStoppable(): Boolean

  /**
   * Initializer of the Protocol.
   * Invoked the first time an [[OnJag]] instantiation.
   * @param sc
   */
  def init(sc: SparkContext): Unit

  /**
   * Global logic entry-point before a superstep starts
   * @param sc Spark context
   * @param superstep the superstep that will be executed
   */
  def beforeSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit

  /**
   *  * Global logic entry-point after a superstep finishes
   * @param sc Spark context
   * @param superstep the just ended superstep
   */
  def afterSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit
}
