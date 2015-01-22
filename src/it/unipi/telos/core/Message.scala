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

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 11/10/13
 *
 * Generic message interface for the ONJAG framework
 */
trait Message[K] extends Serializable {
  val sourceId: K
  val sourceProtocol: String
  val targetId: K
  val targetProtocol: String
}

/**
 * Utility message which encapsulates descriptors
 */
abstract class GraphMessage[K, V <: Any](val sourceId: K,
                                         val sourceProtocol: String,
                                         val targetId: K,
                                         val targetProtocol: String,
                                         val descriptors: Array[V]
                                          )
  extends Message[K] {}

/**
 * Request Framework Message
 *
 * Requests to the framework to retrieve another vertex's context
 * @param sourceId Id of the vertex sender
 * @param targetId Id of the target vertex
 * @param targetProtocol the protocol's name of the context to retrieve
 * @tparam K
 */
final class RequestVertexContextMessage[K](val sourceId: K,
                                           val targetId: K,
                                           val targetProtocol: String) extends Serializable {}

/**
 * Response Framework Message
 *
 * Response from the framework to receive another vertex's context
 * @param sourceId Id of the other vertex
 * @param sourceProtocol protocol context's name of the other vertex
 * @param targetId Id of the receiver (so who ask for that message with a [[RequestVertexContextMessage]]
 * @param cxt The context of the other vertex
 * @tparam K
 */
final class ResponseVertexContextMessage[K](val sourceId: K,
                                            val sourceProtocol: String,
                                            val targetId: K,
                                            val cxt: ProtocolVertexContext[K]) extends Serializable {}


