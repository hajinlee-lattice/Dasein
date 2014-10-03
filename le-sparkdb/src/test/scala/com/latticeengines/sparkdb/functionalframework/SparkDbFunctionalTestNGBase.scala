package com.latticeengines.sparkdb.functionalframework

import com.latticeengines.common.exposed.util.HdfsUtils
import org.apache.hadoop.conf.Configuration
import org.springframework.util.StringUtils
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests
import org.springframework.test.context.ContextConfiguration
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry


@ContextConfiguration(locations = Array("classpath:test-sparkdb-context.xml"))
class SparkDbFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @throws[Exception]
    def doCopy(conf: Configuration, copyEntries: List[Pair[String, String]]) {
      for (entry <- copyEntries) {
        HdfsUtils.copyLocalToHdfs(conf, entry._1, entry._2)
      }
      
    }


}