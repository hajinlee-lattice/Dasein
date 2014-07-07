package com.latticeengines.sparkdb.exposed.service.impl

import org.springframework.beans.factory.annotation.Autowired
import com.latticeengines.sparkdb.exposed.service.SparkLauncherService

import com.latticeengines.sparkdb.functionalframework.SparkDbFunctionalTestNGBase
import org.testng.annotations.Test
import org.testng.Assert._

class SparkLauncherServiceImplTestNG extends SparkDbFunctionalTestNGBase {

  @Autowired
  var sparkLauncherService: SparkLauncherService = null
  
  @Test(groups = Array("functional"))
  def test() = {
    assertNotNull(sparkLauncherService)
  }
}