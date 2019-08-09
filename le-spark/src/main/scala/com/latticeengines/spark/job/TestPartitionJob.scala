package com.latticeengines.spark.job

import com.latticeengines.domain.exposed.spark.TestPartitionJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestPartitionJob extends AbstractSparkJob[TestPartitionJobConfig]{

  override def runJob(spark: SparkSession, lattice: LatticeContext[TestPartitionJobConfig]): Unit = {
    if(lattice.config.getPartition==true){
      setPartitionTargets(0, Seq("Field1","Field2","Field3","Field4","Field5"), lattice)
    }
    val result = lattice.input.head

    // finish
    lattice.output = result :: Nil
    lattice.outputStr = "This is my output!"
  }
}
