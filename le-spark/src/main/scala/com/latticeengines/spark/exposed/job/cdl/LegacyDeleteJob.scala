package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.cdl.CleanupOperationType
import com.latticeengines.domain.exposed.query.BusinessEntity
import com.latticeengines.domain.exposed.spark.cdl.LegacyDeleteJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions.{col, lit, min, map}
import org.apache.spark.sql.{DataFrame, SparkSession}

class LegacyDeleteJob extends AbstractSparkJob[LegacyDeleteJobConfig] {

  private val dummyColumn: String = "DJ_f8f7d5c7"
  private val aggregate_prefix = "AGGR_"

  override def runJob(spark: SparkSession, lattice: LatticeContext[LegacyDeleteJobConfig]): Unit = {
    val config: LegacyDeleteJobConfig = lattice.config
    val deleteSrcIdx: Int = if (config.getDeleteSourceIdx == null) 1 else config.getDeleteSourceIdx.toInt
    val originalSrcIdx: Int = (deleteSrcIdx + 1) % 2
    val joinColumn = config.getJoinedColumns
    val entity = config.getBusinessEntity
    val operationType = config.getOperationType

    val delete: DataFrame = lattice.input(deleteSrcIdx)
    val original: DataFrame = lattice.input(originalSrcIdx)
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    // calculation
    val result = entity match {
      case BusinessEntity.Account
        if operationType.equals(CleanupOperationType.BYUPLOAD_ID) =>
        original.alias("original")
          .join(delete, Seq(joinColumn.getAccountId), "left")
          .where(delete.col(joinColumn.getAccountId).isNull)
          .select("original.*")
      case BusinessEntity.Contact
        if operationType.equals(CleanupOperationType.BYUPLOAD_ID) =>
        original.alias("original")
          .join(delete, Seq(joinColumn.getContactId), "left")
          .where(delete.col(joinColumn.getContactId).isNull)
          .select("original.*")
      case BusinessEntity.Transaction =>
        getDeleteResultByTransaction(operationType, original, delete, joinColumn, config)
    }

    // finish
    lattice.output = result :: Nil
  }

  private def getDeleteResultByTransaction(operationType: CleanupOperationType, original: DataFrame,
                                           delete: DataFrame, joinColumn: LegacyDeleteJobConfig.JoinedColumns, config: LegacyDeleteJobConfig)
  : DataFrame = {
    operationType match {
      case CleanupOperationType.BYUPLOAD_ACPD =>
        val tempResult = original.alias("original")
          .join(delete, Seq(joinColumn.getAccountId, joinColumn.getProductId, joinColumn.getTransactionTime), "left")
        val partA = tempResult.where(delete.col(joinColumn.getAccountId).isNull).select("original.*")
        val partB = tempResult.filter(delete.col(joinColumn.getContactId).isNotNull && original.col(joinColumn
          .getContactId).notEqual(delete.col(joinColumn.getContactId)))
          .select("original.*")
        MergeUtils.concat2(partA, partB)
      case CleanupOperationType.BYUPLOAD_MINDATE =>
        val fields = original.columns.to
        val tempDel = delete.where(delete.col(joinColumn.getTransactionTime).isNotNull)
          .withColumn(dummyColumn, lit("dummyId")).groupBy(col(dummyColumn), col(joinColumn
          .getTransactionTime)).agg(min(joinColumn.getTransactionTime).as(aggregate_prefix.concat(joinColumn.getTransactionTime)))
          .limit(1)
        val tempOriginal = original.withColumn(dummyColumn, lit("dummyId"))
        tempOriginal.show()
        tempOriginal.join(tempDel, Seq(dummyColumn), "left")
        .where(tempOriginal.col(joinColumn.getTransactionTime) < tempDel.col(aggregate_prefix.concat(joinColumn.getTransactionTime))).select(fields map
        tempOriginal.col:_*)
      case CleanupOperationType.BYUPLOAD_MINDATEANDACCOUNT =>
        val tempDel = delete.where(col(joinColumn.getTransactionTime) > 0).groupBy(col(joinColumn.getAccountId)).agg(min
        (joinColumn.getTransactionTime).as(aggregate_prefix.concat(joinColumn.getTransactionTime)))
        val partA = original.alias("original").join(tempDel, Seq(joinColumn.getAccountId), "left").where(tempDel.col
        (joinColumn.getAccountId).isNull).select("original.*")
        val partB = original.alias("original").join(tempDel, Seq(joinColumn.getAccountId), "inner").where(original
          .col(joinColumn.getTransactionTime) < tempDel.col(aggregate_prefix.concat(joinColumn.getTransactionTime)))
          .select("original.*")
        MergeUtils.concat2(partA, partB)
    }
  }

  //val toDateOnlyFromMillis: UserDefinedFunction = udf((time: String) => DateTimeUtils.toDateOnlyFromMillis(time))

  //val dateToDayPeriod: UserDefinedFunction = udf((dateString: String) => DateTimeUtils.dateToDayPeriod(dateString))
}

