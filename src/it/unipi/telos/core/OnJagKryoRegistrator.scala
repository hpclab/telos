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

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 05/03/14.
 */
class OnJagKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Aggregator[_, _, _]])
    kryo.register(classOf[CheckpointCondition])
    kryo.register(classOf[SuperstepIntervalCondition])
    kryo.register(classOf[Combiner[_]])
    kryo.register(classOf[DefaultCombiner[_]])
    kryo.register(classOf[GraphMessage[_, _]])
    kryo.register(classOf[Message[_]])
    kryo.register(classOf[RequestVertexContextMessage[_]])
    kryo.register(classOf[ResponseVertexContextMessage[_]])
    kryo.register(classOf[OnJag])
    kryo.register(classOf[PersistenceMode.Value])
    kryo.register(classOf[Protocol])
    kryo.register(classOf[EmptyProtocolVertexContext[_]])
    kryo.register(classOf[GraphVertexContext[_]])
    kryo.register(classOf[ProtocolVertexContext[_]])
    kryo.register(classOf[SystemProtocolVertexContext[_]])
    kryo.register(classOf[Vertex[_]])
  }
}
