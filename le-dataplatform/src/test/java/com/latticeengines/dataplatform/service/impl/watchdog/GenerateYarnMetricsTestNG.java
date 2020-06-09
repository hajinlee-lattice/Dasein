package com.latticeengines.dataplatform.service.impl.watchdog;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.yarn.exposed.service.YarnService;
public class GenerateYarnMetricsTestNG extends DataPlatformFunctionalTestNGBase {

    @Inject
    private GenerateYarnMetrics generateYarnMetrics;

    @Inject
    private YarnService yarnService;

    @BeforeClass(groups = { "functional" }, enabled = false)
    public void setup() throws Exception {
        generateYarnMetrics.setYarnService(yarnService);
    }

    @AfterClass(groups = { "functional" }, enabled = false)
    public void tearDown() throws Exception {
    }

    @Test(groups = { "functional" }, enabled = false)
    public void testGenerateMetrics() throws Exception {
        generateYarnMetrics.run();
    }
}
