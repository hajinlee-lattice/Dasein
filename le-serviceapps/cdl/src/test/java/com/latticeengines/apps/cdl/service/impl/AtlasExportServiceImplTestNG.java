package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.service.S3ExportFolderService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataSegmentExportConverter;

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

        AtlasExport atlasExport1 = createAtlasExport(AtlasExportType.ACCOUNT);
        verifyAtlasExport(atlasExport1, AtlasExportType.ACCOUNT, MetadataSegmentExport.Status.RUNNING);
        AtlasExport atlasExport2 = createAtlasExport(AtlasExportType.CONTACT);
        verifyAtlasExport(atlasExport2, AtlasExportType.CONTACT, MetadataSegmentExport.Status.RUNNING);

        atlasExport1.setStatus(MetadataSegmentExport.Status.COMPLETED);
        atlasExportService.updateAtlasExport(mainCustomerSpace, atlasExport1);
        atlasExport1 = atlasExportService.getAtlasExport(mainCustomerSpace, atlasExport1.getUuid());
        verifyAtlasExport(atlasExport1, AtlasExportType.ACCOUNT, MetadataSegmentExport.Status.COMPLETED);

        List<AtlasExport> atlasExports = atlasExportService.findAll(mainCustomerSpace);
        Assert.assertEquals(atlasExports.size(), 3);
    }

    private void verifyAtlasExport(AtlasExport atlasExport, AtlasExportType atlasExportType,
                                   MetadataSegmentExport.Status status) {
        Assert.assertNotNull(atlasExport.getUuid());
        Assert.assertNotNull(atlasExport.getDatePrefix());
        Assert.assertEquals(atlasExport.getExportType(), atlasExportType);
        Assert.assertNotNull(atlasExport.getAccountFrontEndRestriction());
        Assert.assertNotNull(atlasExport.getContactFrontEndRestriction());
        Assert.assertNull(atlasExport.getAccountFrontEndRestriction().getRestriction());
        Assert.assertNull(atlasExport.getContactFrontEndRestriction().getRestriction());
        Assert.assertEquals(atlasExport.getCreatedBy(), "default@lattice-engines.com");
        Assert.assertEquals(atlasExport.getStatus(), status);
        Assert.assertNull(atlasExport.getPath());
        Tenant tenant = atlasExport.getTenant();
        Assert.assertEquals(tenant.getId(), mainCustomerSpace);
    }

    private AtlasExport createAtlasExport(AtlasExportType atlasExportType) {
        AtlasExport atlasExport = new AtlasExport();
        atlasExport.setCreatedBy("default@lattice-engines.com");
        atlasExport.setAccountFrontEndRestriction(new FrontEndRestriction());
        atlasExport.setContactFrontEndRestriction(new FrontEndRestriction());
        atlasExport.setApplicationId(UUID.randomUUID().toString());
        atlasExport.setExportType(atlasExportType);
        atlasExport = atlasExportService.createAtlasExport(mainCustomerSpace, atlasExport);
        return atlasExport;
    }
}
