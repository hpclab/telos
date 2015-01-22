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

import scala.Array.canBuildFrom
import scala.reflect.ClassTag

import org.apache.spark.SparkContext

import it.unipi.telos.util.TelosPropertiesImmutable

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 14/05/2014.
 *
 * T-MAN instantiations with ranking function as euclidean distance metric in N-dimensional space
 */
class EuclideanSpaceTMANProtocol(dimension: Int, c: Int, H: Int, R: Int, startStep: Int = 0, step: Int = 1)
  extends TMANProtocol(c, H, R, startStep, step, "euclideantman") {

  override def createProtocolVertexContext[K: ClassTag](id: K, data: Array[Any]): TMANVertexContext[K] = {
    val cxt = new EuclideanSpaceVertexContext[K](data.slice(0, dimension).map(x => x.asInstanceOf[Double]))
    val viewSize = if (data.length - dimension <= c) data.length - dimension else c
    cxt.view = new cxt.viewType(viewSize)
    for (i <- dimension until data.length if i - dimension < c) {
      cxt.view(i - dimension) = ((data(i).asInstanceOf[K], None, 0))
    }
    cxt
  }

  override def beforeSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()

  override def afterSuperstep(sc: SparkContext, superstep: Int, property : TelosPropertiesImmutable): Unit = ()
}

class EuclideanSpaceVertexContext[K](axisValue: Array[Double]) extends TMANVertexContext[K] {
  override type descriptorType = Seq[Double]
  override var descriptor: descriptorType = axisValue
  override var view: viewType = new viewType(0)

  /**
   * The TMAN Ranking Function
   * @param d1 V Descriptor
   * @param d2 Another Descriptor
   * @return rank between d1 and d2
   */
  override def rankingFunction(d1: descriptorType, d2: descriptorType): Double = {
    math.sqrt(d1.zip(d2).map { case (a, b) => math.pow(a - b, 2)}.reduce(_ + _))
  }
}
