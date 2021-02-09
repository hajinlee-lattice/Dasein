package com.latticeengines.elasticsearch.framework;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-elasticsearch-context.xml"})
public class ElasticSearchFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @BeforeMethod(groups = { "functional" })
    public void setup() throws Exception {
    }
}
