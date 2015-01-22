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

import scala.reflect.ClassTag

import org.apache.spark.SparkContext

import it.unipi.telos.core.ProtocolVertexContext
import it.unipi.telos.util.TelosPropertiesImmutable

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 23/10/13.
 *
 * T-MAN instantiations with ranking function defining a geometric ring
 */
class RingTMANProtocol(c: Int, H: Int, R: Int, startStep: Int = 0, step: Int = 1)
  extends TMANProtocol(c, H, R, startStep, step, "ringtman") {

  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): RingTMANVertexContext[K] = {
    val cxt = new RingTMANVertexContext[K](id.toString.toDouble, data(0).asInstanceOf[Long])
    val viewSize = if (data.length - 1 <= c) data.length - 1 else c
    cxt.view = new cxt.viewType(viewSize)
    for (i <- 1 until data.length if i - 1 < c) {
      cxt.view(i - 1) = ((data(i).asInstanceOf[K], None, 0.asInstanceOf[RingTMANVertexContext[K]#ageType]))
    }
    cxt
  }

  def afterSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  def beforeSuperstep(sc: SparkContext, superstep: Int,property : TelosPropertiesImmutable): Unit = ()
}

class RingTMANVertexContext[K](value: Double, N: Long) extends TMANVertexContext[K] with Cloneable {

  type descriptorType = Double

  var descriptor: RingTMANVertexContext[K]#descriptorType = value
  var view: RingTMANVertexContext[K]#viewType = new viewType(0)

  override def clone(): RingTMANVertexContext[K] = {
    val dolly = new RingTMANVertexContext[K](descriptor, N)
    dolly
  }

  override def sendable(): ProtocolVertexContext[K] = {
    val dolly = clone()
    super.fixSendable(dolly)
  }

  def rankingFunction(d1: RingTMANVertexContext[K]#descriptorType, d2: RingTMANVertexContext[K]#descriptorType)
  : Double = {
    val diff = math.abs(d1 - d2)
    math.min(N - diff, diff)
  }
}
