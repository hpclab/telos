/**
 * Copyright 2014 Andrea Esposito <and1989@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.unipi.normalization

import scala.collection.mutable
import java.io.PrintWriter

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 06/11/13.
 */
object Normalizer {

  
  def main(args: Array[String]) {
    // K-Core graphs
    /*
    normalizeSnap("datasets/CA-CondMat")
    normalizeSnap("datasets/Amazon0601")
    normalizeSnap("datasets/CA-AstroPh")
    normalizeSnap("datasets/p2p-Gnutella31")
    normalizeSnap("datasets/roadNet-TX")
    normalizeSnap("datasets/Slashdot0902")
    normalizeSnap("datasets/soc-sign-Slashdot090221")
    normalizeSnap("datasets/web-BerkStan")
    normalizeSnap("datasets/WikiTalk")
    */

    // P2PRandomPeerSampling's graph
    //normalizeSnap("datasets/randompeersampling_validation")

    // TMAN's graphs
    normalizeSnap("run/500000.edgelist")
    //normalizeSnap("datasets/testImage-table", ",")
    //normalizeSnap("datasets/torus-table", ",")

    // JaBeJa's graphs
    //normalizeChaco("datasets/add20")
    //normalizeChaco("datasets/3elt")
    //normalizeChaco("datasets/4elt")
    //normalizeChaco("datasets/vibrobox")
//    normalizeChaco("datasets/twitter", 2) // For twitter because of a mistake in the original dataset put index = 2
//    normalizeChaco("run/cs4")
//    normalizeChaco("run/wave")// For twitter because of a mistake in the original dataset put index = 2
    //normalizeChaco("datasets/facebook")
  }
  

  def normalizeChaco(nameDataset: String, startIndex: Int = 1) {
    var index = startIndex
    val normFile = new PrintWriter(nameDataset + "_normalized.csv")
    val it = scala.io.Source.fromFile(nameDataset + ".graph").getLines()
    it.next() // Drop #Verts and #Edges
    for (line <- it) {
      val field = line.substring(1, line.length).split(" ")
      normFile.write("%s\t%s\n".format(index, field.mkString(",")))
      index += 1
    }
    normFile.close()
  }

  def normalizeSnap(nameDataset: String, separator: String = "\t") {
    val map = new mutable.HashMap[String, List[String]]()
    scala.io.Source.fromFile(nameDataset ).getLines().foreach {
      line =>
        val field = line.split(separator)
        for (i <- 0 to 1)
          map.get(field(i)) match {
            case Some(record) =>
              map.put(field(i), record :+ field(1 - i))
            case None => map.put(field(i), List(field(1 - i)))
          }
    }

    val normFile = new PrintWriter(nameDataset + "_normalized.csv")
    map.keys.foreach(k =>
      map.get(k) match {
        case Some(record) =>
          normFile.write("%s\t%s\n".format(k, record.mkString(",")))
        //print("%s\t%s\n".format(k, record.mkString(",")))
        case None => throw new IllegalStateException()
      }
    )
    normFile.close()
  }
}
