package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

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
        assertNotNull(exportRecord);
        String exportUuid = exportRecord.getUuid();
        assertNotNull(exportUuid);
        assertNotNull(exportRecord.getDatePrefix());
        String dropfolderExportPath = s3ExportFolderService.getDropFolderExportPath(mainCustomerSpace,
                AtlasExportType.ALL_ACCOUNTS, exportRecord.getDatePrefix(), "");
        assertNotNull(dropfolderExportPath);

        List<String> files = new ArrayList<>();
        files.add("TestFile.csv.gz");
        atlasExportService.addFileToDropFolder(mainCustomerSpace, exportUuid, "TestFile.csv.gz", files);
        String systemExportPath = s3ExportFolderService.getSystemExportPath(mainCustomerSpace);
        assertNotNull(systemExportPath);
        files = new ArrayList<>();
        files.add("Account_" + exportUuid + ".csv.gz");
        atlasExportService.addFileToSystemPath(mainCustomerSpace, exportUuid, "Account_" + exportUuid + ".csv.gz", files);
        exportRecord = atlasExportService.getAtlasExport(mainCustomerSpace, exportUuid);
        assertEquals(exportRecord.getFilesUnderDropFolder().size(), 1);
        assertEquals(exportRecord.getFilesUnderSystemPath().size(), 1);
        assertEquals(exportRecord.getFilesUnderDropFolder().get(0), "TestFile.csv.gz");
        assertEquals(exportRecord.getFilesUnderSystemPath().get(0), "Account_" + exportUuid + ".csv.gz");
        atlasExportService.updateAtlasExport(mainCustomerSpace, exportRecord);

        AtlasExport atlasExport1 = createAtlasExport(AtlasExportType.ACCOUNT);
        atlasExport1 = atlasExportService.getAtlasExport(mainCustomerSpace, atlasExport1.getUuid());
        verifyAtlasExport(atlasExport1, AtlasExportType.ACCOUNT, MetadataSegmentExport.Status.RUNNING, null);
        AtlasExport atlasExport2 = createAtlasExport(AtlasExportType.CONTACT);
        verifyAtlasExport(atlasExport2, AtlasExportType.CONTACT, MetadataSegmentExport.Status.RUNNING, null);

        atlasExport1.setStatus(MetadataSegmentExport.Status.COMPLETED);
        String attributeSetName = UUID.randomUUID().toString();
        atlasExport1.setAttributeSetName(attributeSetName);
        atlasExportService.updateAtlasExport(mainCustomerSpace, atlasExport1);
        atlasExport1 = atlasExportService.getAtlasExport(mainCustomerSpace, atlasExport1.getUuid());
        verifyAtlasExport(atlasExport1, AtlasExportType.ACCOUNT, MetadataSegmentExport.Status.COMPLETED, attributeSetName);

        List<AtlasExport> atlasExports = atlasExportService.findAll(mainCustomerSpace);
        assertEquals(atlasExports.size(), 3);
        atlasExportService.deleteAtlasExport(mainCustomerSpace, atlasExport1.getUuid());
        atlasExports = atlasExportService.findAll(mainCustomerSpace);
        assertEquals(atlasExports.size(), 2);
    }

    private void verifyAtlasExport(AtlasExport atlasExport, AtlasExportType atlasExportType,
                                   MetadataSegmentExport.Status status, String attributeSetName) {
        assertNotNull(atlasExport.getUuid());
        assertNotNull(atlasExport.getDatePrefix());
        assertEquals(atlasExport.getExportType(), atlasExportType);
        assertNotNull(atlasExport.getAccountFrontEndRestriction());
        assertNotNull(atlasExport.getContactFrontEndRestriction());
        assertNull(atlasExport.getAccountFrontEndRestriction().getRestriction());
        assertNull(atlasExport.getContactFrontEndRestriction().getRestriction());
        assertEquals(atlasExport.getCreatedBy(), "default@lattice-engines.com");
        assertEquals(atlasExport.getStatus(), status);
        Tenant tenant = atlasExport.getTenant();
        assertEquals(tenant.getId(), mainCustomerSpace);
        assertNull(atlasExport.getFilesToDelete());
        assertNotNull(atlasExport.getUpdated());
        assertNotNull(atlasExport.getCreated());
        assertEquals(atlasExport.getAttributeSetName(), attributeSetName);
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
