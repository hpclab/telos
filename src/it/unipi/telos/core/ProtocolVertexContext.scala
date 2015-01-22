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

import org.apache.spark.broadcast.Broadcast

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 11/10/13
 *
 * Generic vertex context relatives to a protocol
 */
trait ProtocolVertexContext[K] extends Serializable {
  private[core] var indexes: Broadcast[Array[String]] = _
  private[core] var protocols: Array[ProtocolVertexContext[K]] = _

  /**
   * Intra-communication API
   *
   * The context is able to access to the other protocols over the same vertex
   *
   * @param pName Protocol's name
   * @return Vertex context of the requested protocol
   */
  final def accessProtocol(pName: String): Option[ProtocolVertexContext[K]] = {
    val index = indexes.value.indexOf(pName)
    if (index >= 0)
      Some(protocols(index))
    else
      None
  }

  /**
   *
   * @return Vertex's Id
   */
  def getId(): K = id() // Sugar Syntax + Hide Id retrievement

  /**
   * Internal System communication retrieving the vertex's id
   */
  private var id: () => K = {
    () => this.accessProtocol(OnJag.ONJAG_SystemContextName) match {
      case Some(cp) => cp.asInstanceOf[SystemProtocolVertexContext[K]].sysId
      case None => throw new IllegalStateException("Base Layer not found. Maybe the context hasn't been created or processed by the system (have you called fixSendable method?).")
    }
  }

  /**
   * Defines a copy of the current vertex context which is possible to send over the network without any issues occurs.
   * The context has to be with the less memory footprint as possible and it must not hold any references to other contexts
   *
   * @return A read-only copy of the context
   */
  def sendable(): ProtocolVertexContext[K] = this

  /**
   * The override of the [[sendable( )]] method requires to restore the internals
   * Thus, the user has to called this function whenever a custom sendable context is created.
   * Indeed in that case the user should also provide a custom implementation of the [[fixSendable( )]] method.
   *
   * Use case:
   * def [[sendable( )]]
   * new_cxt = custom_logic()
   * return [[fixSendable( n e w _ c x t )]]   *
   *
   * @param p   The context to fix
   * @tparam F  Assumed the method is called by a context with the same key type (i.e. F == K)
   * @return The same context but fixed
   */
  def fixSendable[F](p: ProtocolVertexContext[F]): ProtocolVertexContext[F] = {
    val id: F = this.getId().asInstanceOf[F]
    p.id = () => id
    p
  }

  override def toString(): String = {
    this.getId().toString
  }
}

/**
 * System vertex context
 * It provides the vertex's id
 *
 * @param sysId Vertex's Id
 * @tparam K Id's type
 */
private[core] class SystemProtocolVertexContext[K](val sysId: K) extends ProtocolVertexContext[K] {
  def this(sysId: K, indexMap: Broadcast[Array[String]], protocolMap: Array[ProtocolVertexContext[K]]) {
    this(sysId)
    this.indexes = indexMap
    this.protocols = protocolMap
  }
}

/**
 * Simple Empty vertex context
 */
final class EmptyProtocolVertexContext[K] extends ProtocolVertexContext[K] {}

/**
 * Convenient vertex context for view exchanges
 * @tparam K Vertex's Id
 */
trait GraphVertexContext[K] extends ProtocolVertexContext[K] {
  var links: Array[K]
}

