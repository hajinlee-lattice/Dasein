package com.latticeengines.apps.cdl.service.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;

public class CSVDataFeedMetadataServiceImplUnitTestNG {

    private CSVImportConfig csvImportConfig;

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
        importConfig.setFileSource("HDFS");
        csvImportConfig = new CSVImportConfig();
        csvImportConfig.setReportFileName(FILE_NAME);
        csvImportConfig.setReportFileDisplayName(FILE_DISPLAY_NAME);
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        csvDataFeedMetadataServiceImpl = new CSVDataFeedMetadataServiceImpl();
    }

    @Test(groups = "unit")
    public void testgGetCustomerSpace() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getCustomerSpace(csvImportConfig),
                CUSTOMER_SPACE);
    }

    @Test(groups = "unit")
    public void testGetFileName() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getFileName(csvImportConfig), FILE_NAME);
    }

    @Test(groups = "unit")
    public void testGetFileDisplayName() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getFileDisplayName(csvImportConfig),
                FILE_DISPLAY_NAME);
    }

    @Test(groups = "unit")
    public void testGetConnectorConfig() {
        String str = csvDataFeedMetadataServiceImpl.getConnectorConfig(csvImportConfig,
                JOB_IDENTIFIER);
        Assert.assertTrue(str.contains(JOB_IDENTIFIER));
    }
}
