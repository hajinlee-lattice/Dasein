package com.latticeengines.spark.util

import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

private[spark] object DisplayNameUtils {


  def changeToDisplayName(input: DataFrame, disPlayNames: java.util.Map[String, String]): DataFrame = {
    if (MapUtils.isEmpty(disPlayNames)) {
      input
    } else {
      val attrsToRename: Map[String, String] = disPlayNames.asScala.toMap
        .filterKeys(input.columns.contains(_))
      val newAttrs = input.columns.map(c => attrsToRename.getOrElse(c, c))
      input.toDF(newAttrs: _*)
    }
  }

}
