package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.datacloud.dataflow.AttrDimension
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._

private[spark] object DimensionUtils {

    def getRollupPaths(tree: java.util.List[AttrDimension]): java.util.List[java.util.List[AttrDimension]] = {
        com.latticeengines.domain.exposed.util.DimensionUtils.getRollupPaths(tree)
    }

    def getRollupPaths(tree: Seq[AttrDimension]): Seq[Seq[AttrDimension]] = {
        val javaPaths = getRollupPaths(tree.asJava)
        javaPaths.asScala.map(_.asScala)
    }

    def pathToString(path: java.util.List[AttrDimension]): String = StringUtils.join(path, "->")

    def pathToString(path: Seq[AttrDimension]): String = path.map(_.getName).mkString("->")

    def getParentId(path: Seq[AttrDimension]): String = {
        if (path.length == 1) {
            "base"
        } else {
            pathToString(path.reverse.tail.reverse)
        }
    }
}
