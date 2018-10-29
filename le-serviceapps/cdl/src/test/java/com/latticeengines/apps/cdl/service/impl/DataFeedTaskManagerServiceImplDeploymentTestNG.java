package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.util.ArrayList;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.VdbImportConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;

public class DataFeedTaskManagerServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private VdbImportConfig vdbMetadata1;

    private VdbImportConfig vdbMetadata2;

    @Inject
    private DataFeedTaskManagerService dataFeedTaskManagerService;

    @BeforeClass(groups = "deployment")
    public void setup() throws IOException {
        setupTestEnvironment();
        vdbMetadata1 = new VdbImportConfig();
        vdbMetadata1.setVdbLoadTableConfig(JsonUtils.deserialize(
                IOUtils.toString(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("metadata/vdb/test1.json"), "UTF-8"),
                VdbLoadTableConfig.class));
        vdbMetadata1.getVdbLoadTableConfig().setTenantId(mainTestTenant.getId());
        vdbMetadata2 = new VdbImportConfig();
        vdbMetadata2.setVdbLoadTableConfig(JsonUtils.deserialize(
                IOUtils.toString(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("metadata/vdb/test1.json"), "UTF-8"),
                VdbLoadTableConfig.class));
        vdbMetadata2.getVdbLoadTableConfig().setTenantId(mainTestTenant.getId());
        vdbMetadata1.getVdbLoadTableConfig().getMetadataList().forEach(columnMetadata -> {
            if (columnMetadata.getColumnName().equalsIgnoreCase("Country")) {
                columnMetadata.setApprovedUsage(new ArrayList<>());
                columnMetadata.getApprovedUsage().add("Model");
                columnMetadata.setTags(new ArrayList<>());
                columnMetadata.getTags().add("ExcludeFromAll");
            }
        });
        vdbMetadata2.getVdbLoadTableConfig().getMetadataList().forEach(columnMetadata -> {
            if (columnMetadata.getColumnName().equalsIgnoreCase("Country")) {
                columnMetadata.setApprovedUsage(new ArrayList<>());
                columnMetadata.getApprovedUsage().add("Model");
                columnMetadata.setTags(new ArrayList<>());
                columnMetadata.getTags().add("ExcludeFromListView");
            }
            if (columnMetadata.getColumnName().equalsIgnoreCase("Postal_Code")) {
                columnMetadata.setApprovedUsage(new ArrayList<>());
                columnMetadata.getApprovedUsage().add("ModelAndAllInsights");
                columnMetadata.setTags(new ArrayList<>());
                columnMetadata.getTags().add("ExcludeFromListView");
            }
        });
    }

    @Test(groups = "deployment")
    public void testCreateDataFeedTask() {
        String taskId1 = dataFeedTaskManagerService.createDataFeedTask(mainTestTenant.getId(), "TestAccount", "Account",
                "VisiDB", "", "", false, vdbMetadata1);
        Assert.assertNotNull(taskId1);
        String taskId2 = dataFeedTaskManagerService.createDataFeedTask(mainTestTenant.getId(), "TestAccount", "Account",
                "VisiDB", "", "", false, vdbMetadata2);
        Assert.assertEquals(taskId1, taskId2);
    }

}
