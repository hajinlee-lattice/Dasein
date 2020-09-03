package com.latticeengines.apps.cdl.controller;

import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.proxy.exposed.cdl.CDLDanteConfigProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class CDLDanteConfigResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLDanteConfigResourceDeploymentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private CDLDanteConfigProxy cdlDanteConfigProxy;

    private DanteConfigurationDocument danteConfig;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateMetadata(mainCustomerSpace,5);
    }

    @Test(groups = "deployment")
    public void testGetDanteConfig() throws InterruptedException {
        danteConfig = cdlDanteConfigProxy.getDanteConfiguration(MultiTenantContext.getShortTenantId());
        Assert.assertNotNull(danteConfig);
    }
}
