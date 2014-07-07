package com.latticeengines.sparkdb.exposed.service.impl

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.testng.Assert.assertNotNull
import org.testng.annotations.Test

import com.latticeengines.sparkdb.exposed.service.SparkLauncherService
import com.latticeengines.sparkdb.functionalframework.SparkDbFunctionalTestNGBase

class SparkLauncherServiceImplTestNG extends SparkDbFunctionalTestNGBase {

  @Autowired
  var sparkLauncherService: SparkLauncherService = null
  
  @Test(groups = Array("functional"))
  def test() = {
    assertNotNull(sparkLauncherService)
    var appId = sparkLauncherService.runApp("ABCount2", "Priority0")
    println(appId)
  }
}