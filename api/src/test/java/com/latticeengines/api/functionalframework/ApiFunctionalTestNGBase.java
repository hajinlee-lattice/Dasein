package com.latticeengines.api.functionalframework;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-api-context.xml" })
public class ApiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private Configuration yarnConfiguration;

    protected boolean doYarnClusterSetup() {
        return true;
    }

    @BeforeClass(groups = "functional")
    public void setupRunEnvironment() throws Exception {
        if (!doYarnClusterSetup()) {
            return;
        }
        DataPlatformFunctionalTestNGBase platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setupRunEnvironment();
    }
}