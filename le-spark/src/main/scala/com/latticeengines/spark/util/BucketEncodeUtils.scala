package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.datacloud.dataflow._
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

private[spark] object BucketEncodeUtils {

  import com.latticeengines.spark.utils.BucketEncodeUtils

  private def log: Logger = LoggerFactory.getLogger(classOf[BucketEncodeUtils])

  def bucket(value: Any, algo: BucketAlgorithm): Int = {
    if (value == null) {
      0
    } else {
      algo match {
        case _: BooleanBucket => bucketBoolean(value)
        case bkt: CategoricalBucket => bucketCategorical(value, bkt)
        case bkt: IntervalBucket => bucketInterval(value, bkt)
        case bkt: DiscreteBucket => bucketDiscrete(value, bkt)
        case bkt: DateBucket => bucketDate(value, bkt)
        case _ =>
          log.warn(s"Unknown bucket algorithm $algo")
          0
      }
    }
  }

  private def bucketBoolean(value: Any): Int = {
    val str: String = value.toString.toLowerCase
    if (Set("1", "t", "true", "y", "yes").contains(str)) {
      1
    } else if (Set("0", "f", "false", "n", "no").contains(str)) {
      2
    } else {
      log.warn("Cannot parse value " + value + " to a boolean")
      0
    }
  }

  private def bucketCategorical(value: Any, bucket: CategoricalBucket): Int = {
    val categories: List[String] = bucket.getCategories.toList
    val reversedMapping: Map[String, String] =
      if (bucket.getMapping == null) {
        Map()
      } else {
        bucket.getMapping.flatMap(t => {
          val (key, vals) = t
          vals.map((_, key))
        }).toMap
      }
    var thisCategory: String = value.toString.trim
    if (StringUtils.isEmpty(thisCategory)) {
      0
    } else {
      if (reversedMapping.nonEmpty) {
        thisCategory = reversedMapping(thisCategory)
      }
      val idx: Int = categories.indexOf(thisCategory)
      if (idx < 0) {
        log.warn("Did not find a category for value " + value + " from " + StringUtils.join(categories, ", "))
        0
      } else {
        idx + 1
      }
    }
  }

  private def bucketInterval(value: Any, bucket: IntervalBucket): Int = {
    var number: Number = null
    value match {
      case number1: Number => number = number1
      case _ => try number = value.toString.toDouble
      catch {
        case ex: Exception =>
          log.error("Failed to convert value " + value + " to number for an interval bucket.", ex)
          return 0
      }
    }
    val boundaries: List[Number] = bucket.getBoundaries.toList
    var interval: Int = 1
    for (boundary <- boundaries) {
      if (boundary.doubleValue <= number.doubleValue) interval += 1
    }
    interval
  }

  private def bucketDiscrete(value: Any, bucket: DiscreteBucket): Int = {
    if (value == null) return 0
    try {
      var idx: Int = 1
      for (disVal <- bucket.getValues) {
        if (
          (value.isInstanceOf[Integer] && value == disVal.intValue)
            || (value.isInstanceOf[Long] && value == disVal.longValue)
            || (value.isInstanceOf[Float] && value == disVal.floatValue)
            || (value.isInstanceOf[Double] && value == disVal.doubleValue)
        ) {
          return idx
        } else {
          idx += 1
        }
      }
      log.error("Fail to find value " + value.toString + " in discrete bucket")
      0
    } catch {
      case ex: Exception =>
        log.error("Fail to compare value " + value.toString + " with discrete values in bucket", ex)
        0
    }
  }

  // Decide which interval the date value falls into among the default date buckets.
  // Here are the options:
  // BUCKET #  BUCKET NAME    CRITERIA
  // 0         null           date value null, unparsable or negative
  // 1         LAST 7 DAYS    date between current time and 6 days before current time (inclusive)
  // 2         LAST 30 DAYS   date between current time and 29 days before current time (inclusive)
  // 3         LAST 90 DAYS   date between current time and 89 days before current time (inclusive)
  // 4         LAST 180 DAYS  date between current time and 179 days before current time (inclusive)
  // 5         EVER           date either after current time (in the future) or before 179 days ago
  def bucketDate(value: Any, bucket: DateBucket): Int = { // If no value was provided for this Date Attribute, return 0 representing the "null" bucket.
    if (value == null) {
      0
    } else {
      var timestamp: Long = 0L
      value match {
        case l: Long => timestamp = l
        case _ => try timestamp = value.toString.toLong
        catch {
          case ex: Exception =>
            log.error("Failed to convert value " + value + " to a timestamp for a date bucket.", ex)
            return 0
        }
      }
      if (timestamp < 0) { // Return null bucket for negative dates.
        log.error("Negative valued timestamp provided for a date attribute")
        0
      } else if (timestamp > bucket.getCurTimestamp) {
        // Return EVER bucket for future dates (greater than current timestamp) until Future Dates is implemented
        // in PLS-11623.
        bucket.getDateBoundaries.size + 1
      } else {
        val dateBoundaries: List[Long] = bucket.getDateBoundaries.map(_.toLong).toList
        var interval: Int = 1
        for (dateBoundary <- dateBoundaries) {
          if (timestamp < dateBoundary) interval += 1
        }
        interval
      }
    }
  }

}
