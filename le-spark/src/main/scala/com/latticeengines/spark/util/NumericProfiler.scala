package com.latticeengines.spark.util

import org.apache.commons.math3.stat.descriptive.moment.Kurtosis

import scala.collection.mutable
import scala.util.Random


private[spark] class NumericProfiler[T](vals: Seq[T], numBuckets: Int, minBucketSize: Int, randomSeed: Long) {
  private val RANDOM = new Random(randomSeed)

  def findBoundaries(): List[Double] = {
    val dVals = transform(vals.toList)
    val bndIdx = findBoundaryIdx(dVals)
    toBoundaries(dVals, bndIdx)
  }

  private def toBoundaries(dVals: Seq[Double], bndIdx: Seq[Int]): List[Double] = {
    if (bndIdx.length <= 2) {
      findDistinctValueBoundaries(dVals)
    } else {
      bndIdx.slice(1, bndIdx.length - 1).map(dVals).toList
    }
  }

  private def findDistinctValueBoundaries(dVals: Seq[Double]): List[Double] = {
    val distinctVals = dVals.toSet.toList.sorted
    var sortedVals = if (distinctVals.length > 2) distinctVals.slice(1, distinctVals.length - 1) else distinctVals
    for (_ <- 0 until sortedVals.length - (numBuckets - 1)) {
      val (p1, p2) = sortedVals.splitAt(RANDOM.nextInt(sortedVals.length))
      sortedVals = p1 ++ p2.tail
    }
    sortedVals
  }

  /**
    * generate a list of small bins as a first step in bucketing. bins are
    * built from a sorted list. returns: a list of index positions at
    * boundaries of these bins
    */
  private def findBoundaryIdx(dVals: Seq[Double]): Seq[Int] = {
    // if there is not enough data, no bucketing is needed
    if (dVals.length <= minBucketSize) {
      List(0, dVals.length)
    } else {
      val vals = if (isGeoDist(dVals.toArray)) logTransform(dVals) else dVals

      val xMin = vals.head
      val xMax = vals.last
      val boundaries: Array[Double] = Array.ofDim[Double](numBuckets)
      for (i <- 1 until numBuckets) {
        boundaries(i - 1) = xMin + (xMax - xMin) * i / numBuckets
      }
      val bndIdx: mutable.MutableList[Int] = new mutable.MutableList[Int]
      bndIdx += 0
      for (b <- boundaries) {
        val bIdx = bisectLeft(vals, b)
        if ((bIdx - bndIdx.last) >= minBucketSize) {
          bndIdx += bIdx
        }
      }
      if (xMax > bndIdx.last - minBucketSize) {
        bndIdx += vals.length
      } else {
        bndIdx.update(bndIdx.size - 1, vals.length)
      }
      bndIdx.toList
    }
  }

  private def bisectLeft(vals: Seq[Double], key: Double): Int = {
    var bIdx: Int = java.util.Arrays.binarySearch(vals.toArray, key)
    if (bIdx >= 0) {
      while (bIdx > 0 && vals(bIdx) == vals(bIdx - 1)) bIdx -= 1
    } else {
      bIdx = -(bIdx + 1)
    }
    bIdx
  }

  private def transform(vals: Seq[T]): Seq[Double] = {
    vals.map {
      case v: Int => v.toDouble
      case v: Long => v.toDouble
      case v: Float => v.toDouble
      case v: Double => v
      case v => throw new UnsupportedOperationException(s"Unknown type ${v.getClass}")
    } map ProfileUtils.roundTo5
  }

  private def isGeoDist(dVals: Array[Double]): Boolean = {
    val k = new Kurtosis
    k.evaluate(dVals) >= 3.0
  }

  private def logTransform(dVals:Seq[Double]): Seq[Double] = {
    dVals map (d => {
      if (d > 0) {
        math.log10(d)
      } else if (d < 0) {
        -math.log10(-d)
      } else {
        0
      }
    })
  }

}
