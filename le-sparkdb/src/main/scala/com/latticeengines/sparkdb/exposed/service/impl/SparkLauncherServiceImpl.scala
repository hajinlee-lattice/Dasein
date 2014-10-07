package com.latticeengines.sparkdb.exposed.service.impl

import java.io.File

import scala.Array.canBuildFrom

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
    val ws = SparkLauncherServiceImpl.getWorkspace()
    val params = Array("--class", //
        "com.latticeengines.sparkdb.service.impl.AssemblyServiceImpl", //
        "--name", appName, //
        "--queue", queue, //
        "--driver-memory", "1024m", //
        "--addJars", getListOfDependencyJars(s"$ws/ledp/le-sparkdb/target/dependency"), //
        "--jar", s"file:$ws/ledp/le-sparkdb/target/le-sparkdb-1.0.2-SNAPSHOT.jar")
    System.setProperty("SPARK_YARN_MODE", "true")
    System.setProperty("spark.driver.extraJavaOptions", "-XX:PermSize=128m -XX:MaxPermSize=128m -Dsun.io.serialization.extendedDebugInfo=true")
    val sparkConf = new SparkConf()
    val args = new ClientArguments(params, sparkConf)
    new Client(args, conf, sparkConf).runApp()
  }
  
  private def getListOfDependencyJars(baseDir: String): String = {
    val files = new File(baseDir).listFiles().filter(!_.getName().startsWith("spark-assembly"))
    val prependedFiles = files.map(x => "file:" + x.getAbsolutePath())
    val result = ((prependedFiles.tail.foldLeft(new StringBuilder(prependedFiles.head))) {(acc, e) => acc.append(", ").append(e)}).toString()
    result
  }
}

object SparkLauncherServiceImpl {
  def getWorkspace(): String = {
    "/home/rgonzalez/workspace"
  } 
  
  def main(args: Array[String]) {
    val conf = new org.apache.hadoop.yarn.conf.YarnConfiguration()
    conf.setStrings(org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_APPLICATION_CLASSPATH, 
        org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    var appId = new SparkLauncherServiceImpl().runApp(conf, "DataProfileJob", "Priority0")
    println(s"Application Id = $appId")
  }
}