package com.latticeengines.sparkdb.exposed.service.impl

import org.testng.annotations.Test
import com.latticeengines.sparkdb.functionalframework.SparkDbFunctionalTestNGBase

class SparkLauncherServiceImplTestNG extends SparkDbFunctionalTestNGBase {

  @Test(groups = Array("functional"))
  def test() = {
    print("Testing...\n")
  }
}