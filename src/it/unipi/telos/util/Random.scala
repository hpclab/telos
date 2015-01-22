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

package it.unipi.telos.util

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Andrea Esposito <and1989@gmail.com> on 11/04/2014.
 *
 * Class Random provides a pseudorandom number generator (PRNG) designed for use
 * in parallel programming.
 * <P>
 * Class Random generates random numbers by hashing successive counter
 * values. The seed initializes the counter. The hash function is defined in W.
 * Press et al., <I>Numerical Recipes: The Art of Scientific Computing, Third
 * Edition</I> (Cambridge University Press, 2007), page 352. The hash function
 * applied to the counter value <I>i</I> is:
 * <P>
 * <I>x</I> := 3935559000370003845 * <I>i</I> + 2691343689449507681 (mod 2<SUP>64</SUP>)
 * <BR><I>x</I> := <I>x</I> xor (<I>x</I> right-shift 21)
 * <BR><I>x</I> := <I>x</I> xor (<I>x</I> left-shift 37)
 * <BR><I>x</I> := <I>x</I> xor (<I>x</I> right-shift 4)
 * <BR><I>x</I> := 4768777513237032717 * <I>x</I> (mod 2<SUP>64</SUP>)
 * <BR><I>x</I> := <I>x</I> xor (<I>x</I> left-shift 20)
 * <BR><I>x</I> := <I>x</I> xor (<I>x</I> right-shift 41)
 * <BR><I>x</I> := <I>x</I> xor (<I>x</I> left-shift 5)
 * <BR>Return <I>x</I>
 *
 */
class Random(private val seed: Long) extends Serializable {
  private var counter: Long = 0L

  def nextBoolean(): Boolean = next() > 0L

  def nextByte(): Byte = next().toByte

  def nextByte(n: Byte): Byte = fixedRange(n).toByte

  def nextShort(): Short = next().toShort

  def nextShort(n: Short): Short = fixedRange(n).toShort

  def nextInt(): Int = next().toInt

  def nextInt(n: Int): Int = fixedRange(n).toInt

  def nextLong(): Long = next()

  def nextLong(n: Long): Long = fixedRange(n).toLong

  def nextFloat(): Float = (next() * (1.0f / 18446744073709551616.0f) + 0.5f) // 1/(2^64)

  def nextDouble(): Double = (next() * (1.0 / 18446744073709551616.0) + 0.5) // 1/(2^64)

  def nextChar(): Char = next().toChar

  private def fixedRange(n: Long): Double = math.floor(nextDouble * n)

  /**
   * @return Pseudorandom value.
   */
  private def next(): Long = {
    counter += 1
    return Random.hash(seed + counter)
  }
}

object Random {

  /**
   * Return the hash of the given value.
   */
  private[Random] def hash(x: Long): Long = {
    var ret = x
    ret = 3935559000370003845L * ret + 2691343689449507681L
    ret = ret ^ (ret >>> 21)
    ret = ret ^ (ret << 37)
    ret = ret ^ (ret >>> 4)
    ret = 4768777513237032717L * ret
    ret = ret ^ (ret << 20)
    ret = ret ^ (ret >>> 41)
    ret = ret ^ (ret << 5)
    return ret
  }

  def shuffle[A](seq: Seq[A], rnd: Random): Seq[A] = {
    val tempSeq = new ArrayBuffer[A]()
    tempSeq ++= seq
    val dim = seq.length
    for (i <- 0 until dim / 2) {
      val index = rnd.nextInt(dim)
      val temp = tempSeq(index)
      tempSeq(index) = tempSeq(i)
      tempSeq(i) = temp
    }
    tempSeq.toSeq
  }
}
