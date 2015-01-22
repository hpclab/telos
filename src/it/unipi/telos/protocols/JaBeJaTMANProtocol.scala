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
 * Created by Andrea Esposito <and1989@gmail.com> on 12/12/13.
 *
 * T-MAN instantiations with ranking function which calculates the ''bornerness'' of the stains. Further details in the thesis referenced in [[JaBeJaProtocol]].
 */
class JaBeJaTMANProtocol(c: Int, H: Int, R: Int, startStep: Int = 0, step: Int = 1)
  extends TMANProtocol(c, H, R, startStep, step, "jabejatman") {

  def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): TMANVertexContext[K] = {
    val cxt = new JaBeJaTMANVertexContext[K]()
    val viewSize = if (data.length <= c) data.length else c
    cxt.view = new cxt.viewType(viewSize)
    for (i <- 0 until data.length if i < c) {
      cxt.view(i) = (data(i).asInstanceOf[K], None, 0.asInstanceOf[JaBeJaTMANVertexContext[K]#ageType])
    }
    cxt
  }

  def afterSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  def beforeSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()
}

class JaBeJaTMANVertexContext[K]() extends TMANVertexContext[K] {

  type descriptorType = (Int, Double)
  var descriptor: JaBeJaTMANVertexContext[K]#descriptorType = (-1, Double.MaxValue)
  var view: JaBeJaTMANVertexContext[K]#viewType = new viewType(0)

  def rankingFunction(d1: JaBeJaTMANVertexContext[K]#descriptorType, d2: JaBeJaTMANVertexContext[K]#descriptorType): Double = {
    if ((d1._2 == Double.MaxValue || d2._2 == Double.MaxValue) || (d1._1 == d2._1))
      Double.MaxValue
    else math.abs(d1._2 - d2._2)
  }

  override def sendable(): ProtocolVertexContext[K] = {
    val dolly = new JaBeJaTMANVertexContext[K]()
    dolly.descriptor = this.descriptor
    dolly.view = null
    super.fixSendable(dolly)
  }
}
