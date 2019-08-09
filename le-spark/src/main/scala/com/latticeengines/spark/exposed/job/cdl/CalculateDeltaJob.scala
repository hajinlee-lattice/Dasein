package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit
import com.latticeengines.domain.exposed.spark.cdl.CalculateDeltaJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class CalculateDeltaJob extends AbstractSparkJob[CalculateDeltaJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[CalculateDeltaJobConfig]): Unit = {
    val config: CalculateDeltaJobConfig = lattice.config
    val newAccountUniverse = loadHdfsUnit(spark, config.getCurrentAccountUniverse.asInstanceOf[HdfsDataUnit])
    val newContactUniverse = if (config.getCurrentContactUniverse != null) loadHdfsUnit(spark, config.getCurrentAccountUniverse.asInstanceOf[HdfsDataUnit]) else createEmptyContactsDF(spark)
    val previousAccountUniverse = if (config.getPreviousAccountUniverse != null) loadHdfsUnit(spark, config.getPreviousAccountUniverse.asInstanceOf[HdfsDataUnit]) else createEmptyAccountsDF(spark)
    val previousContactUniverse = if (config.getPreviousContactUniverse != null) loadHdfsUnit(spark, config.getPreviousContactUniverse.asInstanceOf[HdfsDataUnit]) else createEmptyContactsDF(spark)
    val selectedDFAlias = "selectionDataFrame"

    val addedAccounts = newAccountUniverse.alias(selectedDFAlias).join(previousAccountUniverse, Seq(InterfaceName.AccountId.name()), "left")
      .where(previousAccountUniverse.col(InterfaceName.AccountId.name()).isNull)
      .select(selectedDFAlias + ".*")

    val removedAccounts = newAccountUniverse.join(previousAccountUniverse.alias(selectedDFAlias), Seq(InterfaceName.AccountId.name()), "right")
      .where(newAccountUniverse.col(InterfaceName.AccountId.name()).isNull)
      .select(selectedDFAlias + ".*")

    val addedContacts = newContactUniverse.alias(selectedDFAlias) //
      .join(newAccountUniverse, Seq(InterfaceName.AccountId.name())) //
      .join(previousContactUniverse, Seq(InterfaceName.ContactId.name()), "left") //
      .where(previousContactUniverse.col(InterfaceName.ContactId.name()).isNull) //
      .select(selectedDFAlias + ".*")

    val removedContacts = newContactUniverse
      .join(newAccountUniverse, Seq(InterfaceName.AccountId.name())) //
      .join(previousContactUniverse.alias(selectedDFAlias), Seq(InterfaceName.ContactId.name()), "right") //
      .where(newContactUniverse.col(InterfaceName.ContactId.name()).isNull) //
      .select(selectedDFAlias + ".*")

    lattice.output = List(addedAccounts, removedAccounts, addedContacts, removedContacts, newAccountUniverse, newContactUniverse)
  }

  def createEmptyContactsDF(spark: SparkSession): DataFrame = {
    val schema = StructType(
      StructField(InterfaceName.ContactId.name(), StringType, nullable = true) ::
        StructField(InterfaceName.CustomerContactId.name(), StringType, nullable = true) ::
        StructField(InterfaceName.AccountId.name(), StringType, nullable = true) :: Nil)

    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  def createEmptyAccountsDF(spark: SparkSession): DataFrame = {
    val schema = StructType(
      StructField(InterfaceName.AccountId.name(), StringType, nullable = true) ::
        StructField(InterfaceName.CustomerAccountId.name(), StringType, nullable = true) :: Nil)

    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

}
