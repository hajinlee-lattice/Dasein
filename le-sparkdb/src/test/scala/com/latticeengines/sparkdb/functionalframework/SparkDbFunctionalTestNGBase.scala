package com.latticeengines.sparkdb.functionalframework

import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.ContextConfiguration

@ContextConfiguration(locations = Array("classpath:test-sparkdb-context.xml"))
class SparkDbFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

}