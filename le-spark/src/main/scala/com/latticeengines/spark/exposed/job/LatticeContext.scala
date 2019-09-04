package com.latticeengines.spark.exposed.job

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.SparkJobConfig
import org.apache.spark.sql.DataFrame

class LatticeContext[C <: SparkJobConfig](val input: List[DataFrame], val config: C, val targets: List[HdfsDataUnit]) {
  var output: List[DataFrame] = List[DataFrame]()
  var outputStr: String = ""
  var orphanViews: List[String] = List()
}
