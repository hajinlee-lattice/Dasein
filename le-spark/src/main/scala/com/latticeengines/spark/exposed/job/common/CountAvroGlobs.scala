package com.latticeengines.spark.exposed.job.common

import com.latticeengines.domain.exposed.spark.common.CountAvroGlobsConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.SparkSession

class CountAvroGlobs extends AbstractSparkJob[CountAvroGlobsConfig]{

  @throws(classOf[UnsupportedOperationException])
  override def runJob(spark: SparkSession, lattice: LatticeContext[CountAvroGlobsConfig]): Unit = {
    val paths: Array[String] = lattice.config.avroGlobs

    if (paths.exists(notAvroFile(_))) {
      throw new UnsupportedOperationException("Only avro files are compatible with this job")
    }
    lattice.outputStr = paths.map(spark.read.format("avro").load(_).count).foldLeft(0L)(_+_).toString
  }

  def notAvroFile(path: String): Boolean = !path.endsWith(".avro")
}
