package com.latticeengines.dataplatform.service.impl.watchdog;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class GenerateYarnMetricsTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private GenerateYarnMetrics generateYarnMetrics;

    @Autowired
    private YarnService yarnService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        generateYarnMetrics.setYarnService(yarnService);
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() throws Exception {
    }

    @Test(groups = { "functional" })
    public void testGenerateMetrics() throws Exception {
        generateYarnMetrics.run(null);
    }
}
