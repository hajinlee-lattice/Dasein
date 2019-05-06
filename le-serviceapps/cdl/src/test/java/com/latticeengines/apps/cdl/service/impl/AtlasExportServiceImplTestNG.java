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
//        atlasExportService.updateAtlasExportDropfolderpath(mainCustomerSpace, exportUuid, dropfolderExportPath);
        String systemExportPath = s3ExportFolderService.getSystemExportPath(mainCustomerSpace);
//        atlasExportService.updateAtlasExportSystemPath(mainCustomerSpace, exportUuid, systemExportPath);
        exportRecord = atlasExportService.getAtlasExport(mainCustomerSpace, exportUuid);
//        Assert.assertEquals(exportRecord.getDropfolderPath(), dropfolderExportPath);
//        Assert.assertEquals(exportRecord.getSystemPath(), systemExportPath);
    }

}
