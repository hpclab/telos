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

import it.unipi.telos.core.ProtocolVertexContext
import it.unipi.telos.core.Protocol



/**
 * Created by Andrea Esposito <and1989@gmail.com> on 17/03/14.
 *
 * Generic abstract class defining a random peer sampling service
 */
abstract class RandomPeerSamplingProtocol extends Protocol{
  override var name = "peersampling"
}

trait RandomPeerSamplingVertexContext[K] extends ProtocolVertexContext[K] {
  def getPeer(): Option[K]
}