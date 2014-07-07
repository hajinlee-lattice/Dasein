package com.latticeengines.sparkdb.exposed.service.impl

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.Client
import org.apache.spark.deploy.yarn.ClientArguments

import com.latticeengines.sparkdb.exposed.service.SparkLauncherService

class SparkLauncherServiceImpl extends SparkLauncherService {
  
  def runApp(appName: String, queue: String): ApplicationId = {
    val params = Array("--class", "com.latticeengines.sparkdb.ABCount", //
        "--name", appName, //
        "--queue", queue, //
        "--jar", "file:/home/rgonzalez/workspace/ledp/le-sparkdb/target/le-sparkdb-1.0.0-SNAPSHOT.jar")
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf()
    val args = new ClientArguments(params, sparkConf)
    new Client(args, sparkConf).runApp()
  }
}

object SparkLauncherServiceImpl {
  def main(args: Array[String]) {
    
    val appId = new SparkLauncherServiceImpl().runApp("ABCount1", "Priority0")
    print(appId)
  }
}