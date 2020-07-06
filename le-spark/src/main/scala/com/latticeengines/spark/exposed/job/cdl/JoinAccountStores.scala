package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.JoinAccountStoresConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._

/**
  * This combines the following 2 steps
  * 1. filtering ConsolidatedAccount and LatticeAccount by a provided attrs list
  * 2. Join them by AccountId
  */
class JoinAccountStores extends AbstractSparkJob[JoinAccountStoresConfig] {

  private val AccountId = InterfaceName.AccountId.name
  private val CDLUpdatedTime = InterfaceName.CDLUpdatedTime.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[JoinAccountStoresConfig]): Unit = {
    val customerTbl: DataFrame = lattice.input.head.repartition(col(AccountId))
    val latticeTbl: DataFrame = lattice.input(1).repartition(col(AccountId))
    val config: JoinAccountStoresConfig = lattice.config
    val retainAttrs = if (config.getRetainAttrs == null) Seq() else config.getRetainAttrs.toSeq

    val lhs = if (retainAttrs.isEmpty) {
      customerTbl
    } else {
      val lhsRetainAttrs = customerTbl.columns.intersect(retainAttrs)
      customerTbl.select(lhsRetainAttrs map col: _*)
    }

    val rhsRetainAttrs =  if (retainAttrs.isEmpty) {
      latticeTbl.columns.diff(lhs.columns) ++ List(AccountId)
    } else {
      latticeTbl.columns.diff(lhs.columns).intersect(retainAttrs) ++ List(AccountId)
    }
    val rhs = if (rhsRetainAttrs.length == latticeTbl.columns.length) {
      latticeTbl
    } else  {
      latticeTbl.select(rhsRetainAttrs map col: _*)
    }

    val joined = lhs.join(rhs, Seq(AccountId), joinType = "left")

    val result = if (customerTbl.columns.contains(CDLUpdatedTime) && latticeTbl.columns.contains(CDLUpdatedTime)) {
      val updatedTime = latticeTbl.select(AccountId, CDLUpdatedTime)
        .withColumnRenamed(CDLUpdatedTime, s"rhs_$CDLUpdatedTime")
      joined.withColumnRenamed(CDLUpdatedTime, s"lhs_$CDLUpdatedTime")
        .join(updatedTime, Seq(AccountId), joinType = "left")
        .withColumn(CDLUpdatedTime,
          when(col(s"rhs_$CDLUpdatedTime").isNull, col(s"lhs_$CDLUpdatedTime"))
            .otherwise(when(
              col(s"lhs_$CDLUpdatedTime").isNull || col(s"rhs_$CDLUpdatedTime") > col(s"lhs_$CDLUpdatedTime") ,
              col(s"rhs_$CDLUpdatedTime"))
              .otherwise(col(s"lhs_$CDLUpdatedTime"))
            )
        )
        .drop(s"rhs_$CDLUpdatedTime", s"lhs_$CDLUpdatedTime")
    } else {
      joined
    }

    lattice.output = List(result)
  }

}
