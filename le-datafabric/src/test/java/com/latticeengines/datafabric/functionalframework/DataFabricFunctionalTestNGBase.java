package com.latticeengines.datafabric.functionalframework;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.datafabric.service.datastore.FabricDataService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-datafabric-context.xml" })
public abstract class DataFabricFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected static final String BASE_DIR = "/Pods/Default/Services/PropData/Sources";

    @Inject
    protected FabricDataService dataService;

    @Inject
    protected Configuration yarnConfiguration;

}
