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

package it.unipi.telos.test

import it.unipi.telos.protocols._
import it.unipi.telos.core._
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import it.unipi.telos.protocols.IdleProtocol

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 06/02/14.
 */
object IdleMain {

  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis

    if (args.length < 6) {
      System.err.println("Usage: app <inputFile> <usePartitioner> <numPartitions> <maxStep> <host> <spark_home> <checkpointDir>")
      System.exit(-1)
    }

    val inputFile = args(0)
    val usePartitioner = args(1).toBoolean
    val numPartitions = args(2).toInt
    val maxStep = args(3).toInt
    val host = args(4)
    val spark_home = args(5)
    val checkpointDir = args(6)

    val sc = new SparkContext(host, "OnJag", spark_home, List("./ONJAG.jar"))
    sc.setCheckpointDir(checkpointDir)

    // Parse the text file into a graph
    val input = sc.textFile(inputFile)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices. They are %d".format(numVertices))

    val idleProtocol = new IdleProtocol()

    val onjag = new OnJag(sc, StorageLevel.MEMORY_AND_DISK_SER, idleProtocol)
    if (maxStep > 0)
      onjag.setMaxStep(maxStep)

    println("Parsing input file...")
    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (id, body) = (fields(0).toInt, fields(1).replace("\\n", "\n"))
      val links = body.split(",").map(strLink => strLink.toInt).map(x => x.asInstanceOf[Any])
      val data = Array(links.asInstanceOf[Array[Any]])
      (id, onjag.createVertex(id, data))
    })

    if (usePartitioner)
      vertices = vertices.partitionBy(new HashPartitioner(sc.defaultParallelism)).cache
    else
      vertices = vertices.cache
    println("Done parsing input file.")

    // Initialization
    val messages = onjag.createInitMessages(vertices, new Array[Any](1))

    val checkpointConditions = List(Some(new SuperstepIntervalCondition(100)))

    println("OnJag Execution Started")
    val result = onjag.run(null, sc, vertices, messages, checkpointConditions, numPartitions)
    println("OnJag Execution Ended")

    val timeTaken = System.currentTimeMillis - startTime
    println(timeTaken)
  }
}

