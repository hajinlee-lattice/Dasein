package com.latticeengines.sparkdb.exposed.service.impl

import com.latticeengines.common.exposed.util.HdfsUtils
import com.latticeengines.sparkdb.exposed.service.SparkLauncherService
import com.latticeengines.sparkdb.functionalframework.SparkDbFunctionalTestNGBase

import java.io.File
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.testng.Assert.assertNotNull
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test

import scala.io.Source

class SparkLauncherServiceImplTestNG extends SparkDbFunctionalTestNGBase {

  @Autowired
  var sparkLauncherService: SparkLauncherService = null
  
  var conf = new YarnConfiguration()
  
  @BeforeClass(groups = Array("functional"))
  def setup() = {
    println(getClass.getResource(".").getPath())
    val inputDir = getClass.getClassLoader.getResource("com/latticeengines/sparkdb/exposed/service/impl").getPath()
    HdfsUtils.rmdir(conf, "/tmp/sources/")
    HdfsUtils.rmdir(conf, "/tmp/result")
    HdfsUtils.rmdir(conf, "/tmp/training")
    HdfsUtils.rmdir(conf, "/tmp/test")
    
    HdfsUtils.mkdir(conf, "/tmp/sources/")
    
    val files = new File(inputDir).listFiles().filter(filter)
    val copyEntries = files.map(p=>(p.getAbsolutePath(), "/tmp/sources"))
    
    doCopy(conf, copyEntries.toList)
  }
  
  def filter(fileName: File) = fileName.getAbsolutePath().endsWith(".avro")

  @Test(groups = Array("functional"))
  def test() = {
    assertNotNull(sparkLauncherService)
    val conf = new YarnConfiguration()
    conf.setStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    val appId = sparkLauncherService.runApp(conf, "ModelingDataFlowJob", "Priority0")
    println(appId)
  }
}