package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.service.S3ExportFolderService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;

public class AtlasExportServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private AtlasExportService atlasExportService;

    @Inject
    private S3ExportFolderService s3ExportFolderService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testAtlasExportCreateAndUpdate() {
        AtlasExport exportRecord = atlasExportService.createAtlasExport(mainCustomerSpace,
                AtlasExportType.ALL_ACCOUNTS);
        Assert.assertNotNull(exportRecord);
        String exportUuid = exportRecord.getUuid();
        Assert.assertNotNull(exportUuid);
        Assert.assertNotNull(exportRecord.getDatePrefix());
        String dropfolderExportPath = s3ExportFolderService.getDropFolderExportPath(mainCustomerSpace,
                AtlasExportType.ALL_ACCOUNTS, exportRecord.getDatePrefix(), "");
        Assert.assertNotNull(dropfolderExportPath);
        atlasExportService.addFileToDropFolder(mainCustomerSpace, exportUuid, "TestFile.csv.gz");
        String systemExportPath = s3ExportFolderService.getSystemExportPath(mainCustomerSpace);
        Assert.assertNotNull(systemExportPath);
        atlasExportService.addFileToSystemPath(mainCustomerSpace, exportUuid, "Account_" + exportUuid + ".csv.gz");
        exportRecord = atlasExportService.getAtlasExport(mainCustomerSpace, exportUuid);
        Assert.assertEquals(exportRecord.getFilesUnderDropFolder().size(), 1);
        Assert.assertEquals(exportRecord.getFilesUnderSystemPath().size(), 1);
        Assert.assertEquals(exportRecord.getFilesUnderDropFolder().get(0), "TestFile.csv.gz");
        Assert.assertEquals(exportRecord.getFilesUnderSystemPath().get(0), "Account_" + exportUuid + ".csv.gz");
    }
}
