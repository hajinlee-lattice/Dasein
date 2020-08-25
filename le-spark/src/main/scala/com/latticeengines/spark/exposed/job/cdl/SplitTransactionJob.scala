package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.common.exposed.util.DateTimeUtils.{dateToDayPeriod, toDateOnlyFromMillis}
import com.latticeengines.domain.exposed.metadata.{InterfaceName, TableRoleInCollection}
import com.latticeengines.domain.exposed.metadata.transaction.ProductType.{Analytic, Spending}
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata.Details
import com.latticeengines.domain.exposed.spark.cdl.{ActivityStoreSparkIOMetadata, SplitTransactionConfig}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SplitTransactionJob extends AbstractSparkJob[SplitTransactionConfig] {

  val TRANSACTION: String = TableRoleInCollection.ConsolidatedRawTransaction.name
  val PRODUCT: String = TableRoleInCollection.ConsolidatedProduct.name
  val TXN_TYPE: Seq[String] = Seq(
    Analytic.name, //
    Spending.name //
  )
  val TXN_DATE_ATTR: String = InterfaceName.TransactionTime.name // epoch

  override def runJob(spark: SparkSession, lattice: LatticeContext[SplitTransactionConfig]): Unit = {
    val rawTransaction: DataFrame = lattice.input.head
    val outputMetadata: ActivityStoreSparkIOMetadata = new ActivityStoreSparkIOMetadata()
    val detailsMap = new util.HashMap[String, Details]()

    val resultColumns: Seq[String] = rawTransaction.columns
    var outputs: Seq[DataFrame] = Seq()
    val getDateUdf = UserDefinedFunction((time: Long) => toDateOnlyFromMillis(time.toString), StringType, Some(Seq(LongType)))
    val getDateIdUdf = UserDefinedFunction((time: Long) => dateToDayPeriod(toDateOnlyFromMillis(time.toString)), IntegerType, Some(Seq(LongType)))
    for (i <- TXN_TYPE.indices) {
      val typeName = TXN_TYPE(i)
      detailsMap.put(typeName, createSimpleDetails(i))
      outputs :+= rawTransaction.filter(col(InterfaceName.ProductType.name) === typeName).select(resultColumns.head, resultColumns.tail: _*)
        .withColumn(InterfaceName.StreamDateId.name, getDateIdUdf(col(TXN_DATE_ATTR))) // int date id
        .withColumn(InterfaceName.__StreamDate.name, getDateUdf(col(TXN_DATE_ATTR))) // yy-mm-dd
        .withColumn(InterfaceName.ProductType.name, lit(typeName))
    }
    outputMetadata.setMetadata(detailsMap)

    for (index <- outputs.indices) {
      setPartitionTargets(index, Seq(InterfaceName.StreamDateId.name), lattice)
    }
    lattice.output = outputs.toList
    lattice.outputStr = serializeJson(outputMetadata)
  }

  private def createSimpleDetails(idx: Int): Details = {
    val details = new Details
    details.setStartIdx(idx)
    details
  }
}
