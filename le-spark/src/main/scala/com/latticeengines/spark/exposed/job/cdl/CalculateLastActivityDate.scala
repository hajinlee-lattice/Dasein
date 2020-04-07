package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName.{AccountId, ContactId, LastActivityDate}
import com.latticeengines.domain.exposed.query.BusinessEntity.{Account, Contact}
import com.latticeengines.domain.exposed.spark.cdl.CalculateLastActivityDateConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._

/**
  * Aggregate EntityId -> LastActivityDate for Account/Contact
  */
class CalculateLastActivityDate extends AbstractSparkJob[CalculateLastActivityDateConfig] {
  override def runJob(spark: SparkSession, lattice: LatticeContext[CalculateLastActivityDateConfig]): Unit = {
    val config: CalculateLastActivityDateConfig = lattice.config
    // account streams
    val accStreamIndices = config.accountStreamInputIndices.asScala.toList
    val ctkStreamIndices = config.contactStreamInputIndices.asScala.toList
    val dateAttrs = config.dateAttrs.asScala
    val purgeTime: Option[java.lang.Long] = Option(config.earliestEventTimeInMillis)

    // calculate last activity for account and contact
    val ctkDf = ctkStreamIndices
      .map { idx => toLastActivityDate(lattice.input(idx), dateAttrs(idx)) }
      .map { df => purgeExpiredEvents(df, purgeTime).select(ContactId.name, AccountId.name, LastActivityDate.name) }
      .reduceOption((accumuDf, df) => accumuDf.unionByName(df))
      .map { df => df.groupBy(ContactId.name).agg(max(LastActivityDate.name).as(LastActivityDate.name)) }
      .map { df =>
        if (config.contactTableIdx != null) {
          df.join(lattice.input(config.contactTableIdx).select(ContactId.name, AccountId.name), Seq(ContactId.name))
        } else {
          df
        }
      }

    val accDf = (accStreamIndices.map(idx => toLastActivityDate(lattice.input(idx), dateAttrs(idx))) ++ ctkDf)
      .map { df => purgeExpiredEvents(df, purgeTime).select(AccountId.name, LastActivityDate.name) }
      .reduceOption((accumuDf, df) => accumuDf.unionByName(df))
      .map { df => df.groupBy(AccountId.name).agg(max(LastActivityDate.name).as(LastActivityDate.name)) }

    val outputs: List[(String, DataFrame)] = Nil ++ accDf.map((Account.name, _)) ++ ctkDf.map((Contact.name, _))
    // output
    lattice.output = outputs.map(_._2)
    // entity -> corresponding output index
    lattice.outputStr = Serialization.write(outputs.zipWithIndex.map(t => (t._1._1, t._2)).toMap)(org.json4s.DefaultFormats)
  }

  private def purgeExpiredEvents(df: DataFrame, earliestEventTimeInMillis: Option[java.lang.Long]): DataFrame = {
    earliestEventTimeInMillis.map(t => df.filter(df.col(LastActivityDate.name).geq(t))).getOrElse(df)
  }

  private def toLastActivityDate(df: DataFrame, dateAttr: String): DataFrame = {
    if (LastActivityDate.name.equals(dateAttr)) {
      df
    } else {
      df.withColumn(LastActivityDate.name, df.col(dateAttr)).drop(dateAttr)
    }
  }
}
