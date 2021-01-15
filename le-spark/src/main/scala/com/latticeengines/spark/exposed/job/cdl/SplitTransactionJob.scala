package com.latticeengines.spark.exposed.job.cdl

import java.util
import com.latticeengines.common.exposed.util.DateTimeUtils.{dateToDayPeriod, toDateOnlyFromMillis}
import com.latticeengines.domain.exposed.metadata.{InterfaceName, TableRoleInCollection}
import com.latticeengines.domain.exposed.metadata.transaction.ProductType.{Analytic, Spending}
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper.Partition
import com.latticeengines.domain.exposed.spark.cdl.{SparkIOMetadataWrapper, SplitTransactionConfig}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.TransactionSparkUtils
import org.apache.commons.collections4.CollectionUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

class SplitTransactionJob extends AbstractSparkJob[SplitTransactionConfig] {

  val TRANSACTION: String = TableRoleInCollection.ConsolidatedRawTransaction.name
  val PRODUCT: String = TableRoleInCollection.ConsolidatedProduct.name
  val PROD_TYPE: Seq[String] = Seq(
    Analytic.name, //
    Spending.name //
  )
  val TXN_DATE_ATTR: String = InterfaceName.TransactionTime.name // epoch
  val TXN_TYPE: String = InterfaceName.TransactionType.name

  override def runJob(spark: SparkSession, lattice: LatticeContext[SplitTransactionConfig]): Unit = {
    val rawTransaction: DataFrame = lattice.input.head
    val outputMetadata: SparkIOMetadataWrapper = new SparkIOMetadataWrapper()
    val detailsMap = new util.HashMap[String, Partition]()
    val retainTypes: Seq[String] = if (CollectionUtils.isEmpty(lattice.config.retainProductType)) PROD_TYPE else lattice.config.retainProductType

    val resultColumns: Seq[String] = rawTransaction.columns
    var outputs: Seq[DataFrame] = Seq()
    val getDateUdf = UserDefinedFunction((time: Long) => toDateOnlyFromMillis(time.toString), StringType, Some(Seq(LongType)))
    val getDateIdUdf = UserDefinedFunction((time: Long) => dateToDayPeriod(toDateOnlyFromMillis(time.toString)), IntegerType, Some(Seq(LongType)))
    for (i <- retainTypes.indices) {
      val typeName = retainTypes(i)
      detailsMap.put(typeName, createSimpleDetails(i))
      val split: DataFrame = rawTransaction.filter(col(InterfaceName.ProductType.name) === typeName).select(resultColumns.head, resultColumns.tail: _*)
        .withColumn(InterfaceName.StreamDateId.name, getDateIdUdf(col(TXN_DATE_ATTR))) // int date id
        .withColumn(InterfaceName.__StreamDate.name, getDateUdf(col(TXN_DATE_ATTR))) // yy-mm-dd
        .withColumn(InterfaceName.ProductType.name, lit(typeName))
      val fillTxnType: DataFrame = if (hasNullTxnType(split)) {
        logSpark("Null value in transactionType")
        if (split.columns.contains(TXN_TYPE)) {
          val tempDf: DataFrame = split.withColumn(TXN_TYPE + "_temp", when(col(TXN_TYPE).isNull, TransactionSparkUtils.DEFAULT_TXN_TYPE).otherwise(col(TXN_TYPE)))
          tempDf.drop(TXN_TYPE).withColumnRenamed(TXN_TYPE + "_temp", TXN_TYPE)
        } else {
          split.withColumn(TXN_TYPE, lit(TransactionSparkUtils.DEFAULT_TXN_TYPE).cast(StringType))
        }
      } else {
        split
      }
      outputs :+= fillTxnType.repartition(200, col(InterfaceName.StreamDateId.name))
    }
    outputMetadata.setMetadata(detailsMap)

    for (index <- outputs.indices) {
      setPartitionTargets(index, Seq(InterfaceName.StreamDateId.name), lattice)
    }
    lattice.output = outputs.toList
    lattice.outputStr = serializeJson(outputMetadata)
  }

  private def createSimpleDetails(idx: Int): Partition = {
    val details = new Partition
    details.setStartIdx(idx)
    details
  }

  private def hasNullTxnType(df: DataFrame): Boolean = {
    !df.columns.contains(TXN_TYPE) ||
      df.select(TXN_TYPE).distinct.rdd.map(r => r(0).asInstanceOf[String]).collect().contains(null)
  }
}
