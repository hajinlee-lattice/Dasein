package com.latticeengines.spark

import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

private[spark] object DeleteUtils {

  /**
    * Serialize time ranges into string
    *
    * @param timeRanges array of time ranges, each time range is a array of [start time, end time]
    * @return serialized string representation
    */
  def serializeTimeRanges(timeRanges: mutable.Iterable[mutable.WrappedArray[Long]]): String = {
    if (timeRanges == null) {
      null
    } else {
      timeRanges.map(_.mkString(",")).mkString("|")
    }
  }

  /**
    * Deserialize time range string
    *
    * @param timeRangesStr serialized time range string representation
    * @return array of time ranges, each time range is a array of [start time, end time]
    */
  def deserializeTimeRanges(timeRangesStr: String): Option[Array[Array[Long]]] = {
    if (StringUtils.isBlank(timeRangesStr)) {
      None
    } else {
      Some(timeRangesStr.split("\\|").map(_.split(",").map(_.toLong)))
    }
  }
}
