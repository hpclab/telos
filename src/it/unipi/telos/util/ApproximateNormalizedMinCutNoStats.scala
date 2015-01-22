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

package it.unipi.telos.util

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.reflect.ClassTag
import it.unipi.telos.core.OnJag
import it.unipi.telos.protocols.FindSubPartitionProtocol
import it.unipi.telos.protocols.JaBeJaVertexContextNoStats
import it.unipi.telos.core.PersistenceMode
import it.unipi.telos.core.Vertex


/**
 * Override SparkContext implicits
 * Comment/Uncomment to deactivate completely the FAKE MODE !!!!!
 */

//import it.unipi.thesis.andrea.esposito.spark.MisfireSparkContext._


/**
 * Created by Andrea Esposito <and1989@gmail.com> on 14/04/2014.
 */
object ApproximateNormalizedMinCutNoStats {


  def measureFromJaBeJa[K: ClassTag](property : TelosPropertiesImmutable,
		  								sc: SparkContext,
                                     jabejaResult: RDD[(K, Vertex[K])],
                                     jabejaIndex: Int,
                                     colors: Int,
                                     alfa: Double = 1.0,
                                     x: Double = 8.0,
                                     y: Double = 2.0,
                                     z: Double = 0.0,
                                     computeDistances: Boolean = false,
                                     distancesSamplingPercentage: Double = 0.005,
                                     numPartition: Option[Int] = None,
                                     storageLevel: StorageLevel = OnJag.DEFAULT_STORAGE_LEVEL,
                                     persistenceMode: PersistenceMode.Value = OnJag.PERSISTENCE_MODE_DEFAULT): Double = {
    val graph = jabejaResult.map(e => {
      val jabejaCxt = e._2.protocolsContext(jabejaIndex).asInstanceOf[JaBeJaVertexContextNoStats[K]]
      (e._1, jabejaCxt.neighbourView.map(v => v._1), jabejaCxt.color)
    })
    measure[K](property, sc, graph, colors, alfa, x, y, z, computeDistances, distancesSamplingPercentage, numPartition, storageLevel, persistenceMode)
  }

  /**
   * Compute the NQcut measure over a given graph
   */
  def measure[K: ClassTag](property : TelosPropertiesImmutable,
		  				sc: SparkContext,
                           graph: RDD[(K, Array[K], Int)],
                           colors: Int,
                           alfa: Double = 1.0,
                           x: Double = 8.0,
                           y: Double = 2.0,
                           z: Double = 0.0,
                           computeDistances: Boolean = false,
                           distancesSamplingPercentage: Double = 0.005,
                           numPartition: Option[Int] = None,
                           storageLevel: StorageLevel = OnJag.DEFAULT_STORAGE_LEVEL,
                           persistenceMode: PersistenceMode.Value = OnJag.PERSISTENCE_MODE_DEFAULT): Double = {
    val onjag = new OnJag(sc, storageLevel, new FindSubPartitionProtocol)
    val verts = graph.map(v => {
      val data = Array[Array[Any]](v._2.+:(v._3))
      (v._1, onjag.createVertex(v._1, data))
    })
    val messages = onjag.createInitMessages(verts, new Array[Any](1))
    val actualNumPartitions = numPartition match {
      case Some(n) => n
      case None => sc.defaultParallelism
    }
    val result = onjag.run(property, sc, verts, messages, actualNumPartitions, persistenceMode)
    val mincut = FindSubPartitionProtocol.computeMincut(result)
    val statistics = FindSubPartitionProtocol.computeSubPartitionStats(result)
    val assoc = FindSubPartitionProtocol.computeAssocValues(result)
    var distances: RDD[(Int, Double)] = assoc.map(partition => (partition._1, 0.0))

    if (computeDistances) {
      //TODO: Compute distances with the sampling % given
    }

    val similarity = assoc.groupWith(statistics, distances).aggregate(0.0)(
      (sum: Double, partition: (Int, (Iterable[(Long, Long)], Iterable[(Int, Double, Double)], Iterable[Double]))) =>
        sum +
          (partition._2._1.head._1.toDouble / partition._2._1.head._2.toDouble) * // assoc(A,A) / assoc(A,V)
            ((x * math.pow(math.log(math.log(partition._2._2.head._1 - 1 + math.E) + math.E), -1) + // ln(n-1+e)^-1
              y * math.pow(math.log(math.log(math.pow(partition._2._2.head._3, alfa) + math.E) + math.E), -1) + // ln(sigma^alfa+e)^-1
              z * math.pow(math.log(math.log(partition._2._3.head + math.E) + math.E), -1) // 2 * ln(NSPD(A)+e)^-1
              ) / (x + y + z))
      ,
      (sumA: Double, sumB: Double) => sumA + sumB
    )

    val similarityOnlyN = assoc.groupWith(statistics, distances).aggregate(0.0)(
      (sum: Double, partition: (Int, (Iterable[(Long, Long)], Iterable[(Int, Double, Double)], Iterable[Double]))) =>
        sum +
          (partition._2._1.head._1.toDouble / partition._2._1.head._2.toDouble) * // assoc(A,A) / assoc(A,V)
            math.pow(math.log(math.log(partition._2._2.head._1 - 1 + math.E) + math.E), -1)
      ,
      (sumA: Double, sumB: Double) => sumA + sumB
    )

    val similarityOnlySigma = assoc.groupWith(statistics, distances).aggregate(0.0)(
      (sum: Double, partition: (Int, (Iterable[(Long, Long)], Iterable[(Int, Double, Double)], Iterable[Double]))) =>
        sum +
          (partition._2._1.head._1.toDouble / partition._2._1.head._2.toDouble) * // assoc(A,A) / assoc(A,V)
            math.pow(math.log(math.log(math.pow(partition._2._2.head._3, alfa) + math.E) + math.E), -1)
      ,
      (sumA: Double, sumB: Double) => sumA + sumB
    )

    println("MINCUT: %s".format(mincut))
    println("Statistics: %s\nAssoc: %s".format(
      statistics.map(s => "(%s, (%s))".format(s._1, s._2)).collect().mkString(" # "),
      assoc.map(a => "(%s, (%s))".format(a._1, a._2)).collect().mkString(" # ")
    ))
    println("Similarity: " + similarity)
    println("Similarity Only N: " + similarityOnlyN)
    println("Similarity Only Sigma: " + similarityOnlySigma)
    println("Ncut Only N: " + (2.0 - similarityOnlyN))
    println("Ncut Only Sigma: " + (2.0 - similarityOnlySigma))

    colors - similarity // NQcut(A,B,...)
  }
}
