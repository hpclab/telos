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

import scala.reflect.ClassTag

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 22/02/14.
 *
 * Pregel-inspired combiners
 */
trait Combiner[K] {
  type M = Message[K]
  type C = Seq[_ <: M]

  def createCombiner(msg: M): C

  def mergeMsg(combiner: C, msg: M): C

  def mergeCombiners(a: C, b: C): C
}

/** Default combiner that simply appends messages together (i.e. performs no aggregation) */
class DefaultCombiner[K: ClassTag] extends Combiner[K] with Serializable {
  def createCombiner(msg: M): Seq[M] =
    Array(msg)

  def mergeMsg(combiner: Seq[M], msg: M): Seq[M] =
    combiner :+ msg

  def mergeCombiners(a: Seq[M], b: Seq[M]): Seq[M] =
    a ++ b
}
