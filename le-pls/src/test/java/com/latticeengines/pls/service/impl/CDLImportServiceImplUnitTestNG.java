package com.latticeengines.pls.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;

public class CDLImportServiceImplUnitTestNG {

    @Spy
    private CDLImportServiceImpl cdlImportServiceImpl = new CDLImportServiceImpl();

    private SourceFile sourceFile;

    private String DISPLAY_NAME = "displayName";

    private String FILE_NAME = "fileName";

    @BeforeClass(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        sourceFile = new SourceFile();
        sourceFile.setTableName("metadataTable");
        sourceFile.setPath("/hdfs/samplePath");
        sourceFile.setDisplayName(DISPLAY_NAME);
        sourceFile.setName(FILE_NAME);
    }

    @Test(groups = "unit")
    public void testGenerateImportConfigStr() {
        doReturn(sourceFile).when(cdlImportServiceImpl).getSourceFile(any(String.class));
        CSVImportConfig csvImportConfig = cdlImportServiceImpl.generateImportConfig("customerSpace", "templateName",
                "dataFileName");
        Assert.assertNotNull(csvImportConfig);
        CSVToHdfsConfiguration importConfig = csvImportConfig.getCsvToHdfsConfiguration();
        Assert.assertEquals(csvImportConfig.getReportFileDisplayName(), DISPLAY_NAME);
        Assert.assertEquals(csvImportConfig.getReportFileName(), FILE_NAME);
    }
}
