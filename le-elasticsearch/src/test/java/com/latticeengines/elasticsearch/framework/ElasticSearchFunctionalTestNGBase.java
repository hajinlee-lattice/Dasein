package com.latticeengines.elasticsearch.framework;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-elasticsearch-context.xml"})
public class ElasticSearchFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

}
