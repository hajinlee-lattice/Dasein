package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.CountProductTypeConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.mutable

class CountProductTypeJob extends AbstractSparkJob[CountProductTypeConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[CountProductTypeConfig]): Unit = {
    val types: Seq[String] = lattice.config.types
    val df: DataFrame = lattice.input.head.select(InterfaceName.ProductType.name)
    val result: mutable.Map[String, Long] = mutable.Map()
    types.foreach(prodType => {
      result.put(prodType, df.filter(col(InterfaceName.ProductType.name)===prodType).count)
    })
    lattice.outputStr = serializeJson(result)
  }
}
