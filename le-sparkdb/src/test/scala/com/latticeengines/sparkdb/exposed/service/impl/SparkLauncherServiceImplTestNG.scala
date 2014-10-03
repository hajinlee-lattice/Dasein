package com.latticeengines.sparkdb.exposed.service.impl

import com.latticeengines.common.exposed.util.HdfsUtils
import com.latticeengines.sparkdb.exposed.service.SparkLauncherService
import com.latticeengines.sparkdb.functionalframework.SparkDbFunctionalTestNGBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.testng.Assert.assertNotNull
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test

class SparkLauncherServiceImplTestNG extends SparkDbFunctionalTestNGBase {

  @Autowired
  var sparkLauncherService: SparkLauncherService = null
  
  var conf = new YarnConfiguration()
  
  @BeforeClass(groups = Array("functional"))
  def setup() = {
    HdfsUtils.rmdir(conf, "/tmp/result")
    HdfsUtils.rmdir(conf, "/tmp/training")
    HdfsUtils.rmdir(conf, "/tmp/test")
  }

  @Test(groups = Array("functional"))
  def test() = {
    assertNotNull(sparkLauncherService)
    val conf = new YarnConfiguration()
    conf.setStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    val appId = sparkLauncherService.runApp(conf, "ModelingDataFlowJob", "Priority0")
    println(appId)
  }
}