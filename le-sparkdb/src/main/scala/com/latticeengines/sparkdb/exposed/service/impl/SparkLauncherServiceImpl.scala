package com.latticeengines.sparkdb.exposed.service.impl

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.Client
import org.apache.spark.deploy.yarn.ClientArguments
import org.springframework.stereotype.Component

import com.latticeengines.sparkdb.exposed.service.SparkLauncherService

@Component("sparkLauncherService")
class SparkLauncherServiceImpl extends SparkLauncherService {
  
  override def runApp(conf: Configuration, appName: String, queue: String): ApplicationId = {
    val params = Array("--class", "com.latticeengines.sparkdb.ABCount", //
        "--name", appName, //
        "--queue", queue, //
        "--jar", "file:/home/rgonzalez/workspace/ledp/le-sparkdb/target/le-sparkdb-1.0.0-SNAPSHOT.jar")
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf()
    val args = new ClientArguments(params, sparkConf)
    new Client(args, conf, sparkConf).runApp()
  }
}

object SparkLauncherServiceImpl {
  def main(args: Array[String]) {
    
  }
}