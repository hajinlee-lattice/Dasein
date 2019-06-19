package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.ExtractInactiveRatingsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._

class ExtractInactiveRatingsJob extends AbstractSparkJob[ExtractInactiveRatingsConfig] {

  private val AccountId = InterfaceName.AccountId.name
  private val CreateTime = InterfaceName.CDLCreatedTime.name
  private val UpdateTime = InterfaceName.CDLUpdatedTime.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[ExtractInactiveRatingsConfig]): Unit = {
    val config: ExtractInactiveRatingsConfig = lattice.config
    val input = lattice.input.head
    val ratingAttrs = config.getInactiveRatingColumns.asScala.toList intersect input.columns
    val allAttrs = List(AccountId, CreateTime, UpdateTime) ::: ratingAttrs

    val selected = input.select(allAttrs map col: _*)
    val filtered = selected.filter(row => {
      // keep accounts with at least one not-null rating
      row.getValuesMap(ratingAttrs).values.exists(v => v != null)
    })

    // finish
    lattice.output = filtered::Nil
  }

}
