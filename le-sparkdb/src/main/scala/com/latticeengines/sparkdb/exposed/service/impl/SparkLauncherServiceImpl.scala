package com.latticeengines.sparkdb.exposed.service.impl

import com.latticeengines.sparkdb.ABCount
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn._
import org.apache.hadoop.yarn.api.records.ApplicationId

class SparkLauncherServiceImpl {
  
  def runApp(): ApplicationId = {
    val params = Array("--class", "com.latticeengines.sparkdb.ABCount", //
        "--name", "ABCount", //
        "--queue", "Priority0", //
        "--jar", "file:/home/rgonzalez/workspace/ledp/le-sparkdb/target/le-sparkdb-1.0.0-SNAPSHOT.jar")
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf()
    val args = new ClientArguments(params, sparkConf)
    new Client(args, sparkConf).runApp()
  }
}

object SparkLauncherServiceImpl {
  def main(args: Array[String]) {
    
    val appId = new SparkLauncherServiceImpl().runApp()
    print(appId)
  }
}