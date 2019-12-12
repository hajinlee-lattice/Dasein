package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.MergeSystemBatchConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col}

import scala.collection.JavaConverters._

class MergeSystemBatchJob extends AbstractSparkJob[MergeSystemBatchConfig] {
  
  override def runJob(spark: SparkSession, lattice: LatticeContext[MergeSystemBatchConfig]): Unit = {
    val config: MergeSystemBatchConfig = lattice.config
    if (config.getSystems == null) {
      lattice.output = lattice.input
      
    } else {
      val systems = config.getSystems.asScala.toList 
      val joinKey = config.getJoinKey
      val overwriteByNull: Boolean =
        if (config.getNotOverwriteByNull == null) true else !config.getNotOverwriteByNull.booleanValue()
      var lhsDf = selectSystemBatch(lattice.input.head, systems(0), joinKey, config.isKeepPrefix)
      if (systems.length == 1) {
          lattice.output = lhsDf :: Nil
              
      } else {
        for ( i <- 1 until systems.length) {
          var rhsDf = selectSystemBatch(lattice.input.head, systems(i), joinKey, config.isKeepPrefix)
          lhsDf = MergeUtils.merge2(lhsDf, rhsDf, Seq(joinKey), Set(), overwriteByNull = overwriteByNull) 
        }
        lattice.output = lhsDf :: Nil
      }
    }
  }

  private def selectSystemBatch(df: DataFrame, system: String, joinKey: String, keepPrefix: Boolean): DataFrame = {
      var fields = scala.collection.mutable.ListBuffer[String]()
      fields += joinKey
      df.columns.foreach{ c =>
        if (c.startsWith(system + "__")) {
          fields += c
        }
      }
      var newDf = df.select(fields map col: _*)
      newDf = removeSystemPrefix(newDf, system, joinKey, keepPrefix)
      newDf.na.drop(2)
  }
  
  private def removeSystemPrefix(df: DataFrame, system: String, joinKey: String, keepPrefix: Boolean): DataFrame = {
    if (keepPrefix) {
      df
    } else {
      val newColumns = 
        df.columns map (c => if (c == joinKey) c else c.stripPrefix(system + "__"))
      return df.toDF(newColumns:_*)
    }
  }
}
