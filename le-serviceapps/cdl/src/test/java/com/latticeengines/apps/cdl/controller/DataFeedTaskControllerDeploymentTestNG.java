package com.latticeengines.apps.cdl.controller;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.VdbImportConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public class DataFeedTaskControllerDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Autowired
    private CDLProxy cdlProxy;

    @BeforeClass(groups = {"deployment"})
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = {"deployment"})
    public void testCreateDataFeedTask() throws IOException {

        VdbLoadTableConfig testVdbMetadata = JsonUtils.deserialize(IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("metadata/vdb/testmetadata.json"),
                "UTF-8"), VdbLoadTableConfig.class);
        VdbImportConfig vdbImportConfig = new VdbImportConfig();
        vdbImportConfig.setVdbLoadTableConfig(testVdbMetadata);
        String taskId = cdlProxy.createDataFeedTask(mainTestTenant.getId(), "VisiDB", "Account", "testQuery",
                vdbImportConfig);
        Assert.assertNotNull(taskId);
    }
}
