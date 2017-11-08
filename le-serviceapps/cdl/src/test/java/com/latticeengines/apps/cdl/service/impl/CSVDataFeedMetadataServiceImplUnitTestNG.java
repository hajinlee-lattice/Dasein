package com.latticeengines.apps.cdl.service.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;

public class CSVDataFeedMetadataServiceImplUnitTestNG {

    private CSVToHdfsConfiguration importConfig;

    private String TENANT_NAME = "CustomerSpace";

    private CustomerSpace CUSTOMER_SPACE = CustomerSpace.parse(TENANT_NAME);

    private String FILE_DISPLAY_NAME = "displayName";

    private String FILE_NAME = "fileName";

    private String JOB_IDENTIFIER = "jobIdentifier";

    private CSVDataFeedMetadataServiceImpl csvDataFeedMetadataServiceImpl;

    @BeforeTest(groups = "unit")
    public void setup() {
        importConfig = new CSVToHdfsConfiguration();
        importConfig.setCustomerSpace(CUSTOMER_SPACE);
        importConfig.setTemplateName("templateName");
        importConfig.setFilePath("filePath");
        importConfig.setFileDisplayName(FILE_DISPLAY_NAME);
        importConfig.setFileName(FILE_NAME);
        importConfig.setFileSource("HDFS");
        csvDataFeedMetadataServiceImpl = new CSVDataFeedMetadataServiceImpl();
    }

    @Test(groups = "unit")
    public void testgGetCustomerSpace() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getCustomerSpace(JsonUtils.serialize(importConfig)),
                CUSTOMER_SPACE);
    }

    @Test(groups = "unit")
    public void testGetFileName() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getFileName(JsonUtils.serialize(importConfig)), FILE_NAME);
    }

    @Test(groups = "unit")
    public void testGetFileDisplayName() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getFileDisplayName(JsonUtils.serialize(importConfig)),
                FILE_DISPLAY_NAME);
    }

    @Test(groups = "unit")
    public void testGetConnectorConfig() {
        String str = csvDataFeedMetadataServiceImpl.getConnectorConfig(JsonUtils.serialize(importConfig),
                JOB_IDENTIFIER);
        Assert.assertTrue(str.contains(JOB_IDENTIFIER));
    }
}
