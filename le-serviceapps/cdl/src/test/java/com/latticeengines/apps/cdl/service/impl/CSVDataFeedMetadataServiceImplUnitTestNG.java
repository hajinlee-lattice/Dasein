package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.Mockito.doReturn;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public class CSVDataFeedMetadataServiceImplUnitTestNG {

    private CSVImportConfig csvImportConfig;

    private CSVImportFileInfo csvImportFileInfo;

    private CSVToHdfsConfiguration importConfig;

    private String TENANT_NAME = "CustomerSpace";

    private CustomerSpace CUSTOMER_SPACE = CustomerSpace.parse(TENANT_NAME);

    private String FILE_DISPLAY_NAME = "displayName";

    private String FILE_NAME = "fileName";

    private String INITIATOR = "test@lattice-engines.com";

    private String JOB_IDENTIFIER = "jobIdentifier";

    @InjectMocks
    @Spy
    private CSVDataFeedMetadataServiceImpl csvDataFeedMetadataServiceImpl;

    @Mock
    private BatonService batonService;

    @BeforeTest(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        importConfig = new CSVToHdfsConfiguration();
        importConfig.setCustomerSpace(CUSTOMER_SPACE);
        importConfig.setTemplateName("templateName");
        importConfig.setFilePath("filePath");
        importConfig.setFileSource("HDFS");
        csvImportFileInfo = new CSVImportFileInfo();
        csvImportConfig = new CSVImportConfig();
        csvImportConfig.setCSVImportFileInfo(csvImportFileInfo);
        csvImportFileInfo.setReportFileName(FILE_NAME);
        csvImportFileInfo.setReportFileDisplayName(FILE_DISPLAY_NAME);
        csvImportFileInfo.setFileUploadInitiator(INITIATOR);
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        doReturn(false).when(batonService).isEnabled(CUSTOMER_SPACE, LatticeFeatureFlag.ENABLE_IMPORT_ERASE_BY_NULL);
    }

    @Test(groups = "unit")
    public void testgGetCustomerSpace() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getCustomerSpace(csvImportConfig), CUSTOMER_SPACE);
    }

    @Test(groups = "unit")
    public void testGetFileUploadInitiator() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getImportFileInfo(csvImportConfig).getFileUploadInitiator(),
                INITIATOR);
    }

    @Test(groups = "unit")
    public void testGetFileName() {
        Assert.assertEquals(csvDataFeedMetadataServiceImpl.getImportFileInfo(csvImportConfig).getReportFileName(),
                FILE_NAME);
    }

    @Test(groups = "unit")
    public void testGetFileDisplayName() {
        Assert.assertEquals(
                csvDataFeedMetadataServiceImpl.getImportFileInfo(csvImportConfig).getReportFileDisplayName(),
                FILE_DISPLAY_NAME);
    }

    @Test(groups = "unit")
    public void testGetConnectorConfig() {
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(JOB_IDENTIFIER);
        String str = csvDataFeedMetadataServiceImpl.getConnectorConfig(csvImportConfig, dataFeedTask);
        Assert.assertTrue(str.contains(JOB_IDENTIFIER));
    }
}
