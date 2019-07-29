package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;

public class AtlasExportResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private AtlasExportService atlasExportService;

    @Inject
    private AtlasExportProxy atlasExportProxy;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    private AtlasExport exportRecord;

    private AtlasExport exportRecord2;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        exportRecord = atlasExportService.createAtlasExport(mainCustomerSpace,
                AtlasExportType.ACCOUNT_AND_CONTACT);
        exportRecord2 = createAtlasExport(AtlasExportType.ACCOUNT);
    }

    @Test(groups = "deployment")
    public void testAtlasExportProxy() {
        AtlasExport atlasExport = atlasExportProxy.findAtlasExportById(mainCustomerSpace, exportRecord.getUuid());
        Assert.assertNotNull(atlasExport);
        String dropFolderPath = atlasExportProxy.getDropFolderExportPath(mainCustomerSpace, atlasExport.getExportType(),
                atlasExport.getDatePrefix(), false);
        String systemPath = atlasExportProxy.getSystemExportPath(mainCustomerSpace, false);
        Assert.assertTrue(s3Service.objectExist(s3Bucket, dropFolderPath));
        Assert.assertTrue(s3Service.objectExist(s3Bucket, systemPath));

        String s3DropFolderPath = atlasExportProxy.getS3PathWithProtocol(mainCustomerSpace, dropFolderPath);
        String s3SystemPath = atlasExportProxy.getS3PathWithProtocol(mainCustomerSpace, systemPath);
        Assert.assertNotNull(s3DropFolderPath);
        Assert.assertNotNull(s3SystemPath);

        atlasExportProxy.addFileToDropFolder(mainCustomerSpace, atlasExport.getUuid(), "Dropfolder-Account.csv.gz");

        atlasExport = atlasExportProxy.findAtlasExportById(mainCustomerSpace, exportRecord.getUuid());
        Assert.assertEquals(atlasExport.getFilesUnderDropFolder().size(), 1);
        atlasExportProxy.addFileToSystemPath(mainCustomerSpace, atlasExport.getUuid(),
                "Account-" + atlasExport.getUuid() + ".csv.gz");
        atlasExport = atlasExportProxy.findAtlasExportById(mainCustomerSpace, exportRecord.getUuid());
        Assert.assertEquals(atlasExport.getFilesUnderSystemPath().size(), 1);
        atlasExport = atlasExportProxy.findAtlasExportById(mainCustomerSpace, exportRecord2.getUuid());
        verifyAtlasExport(atlasExport, AtlasExportType.ACCOUNT, MetadataSegmentExport.Status.RUNNING);
        atlasExportProxy.updateAtlasExport(mainCustomerSpace, exportRecord2.getUuid(), MetadataSegmentExport.Status.COMPLETED);
        atlasExport = atlasExportProxy.findAtlasExportById(mainCustomerSpace, exportRecord2.getUuid());
        verifyAtlasExport(atlasExport, AtlasExportType.ACCOUNT, MetadataSegmentExport.Status.COMPLETED);
        List<AtlasExport> atlasExports = atlasExportProxy.findAll(mainCustomerSpace);
        Assert.assertEquals(atlasExports.size(), 2);

    }

    private AtlasExport createAtlasExport(AtlasExportType atlasExportType) {
        AtlasExport atlasExport = new AtlasExport();
        atlasExport.setCreatedBy("default@lattice-engines.com");
        atlasExport.setAccountFrontEndRestriction(new FrontEndRestriction());
        atlasExport.setContactFrontEndRestriction(new FrontEndRestriction());
        atlasExport.setApplicationId(UUID.randomUUID().toString());
        atlasExport.setExportType(atlasExportType);
        atlasExport = atlasExportProxy.createAtlasExport(mainCustomerSpace, atlasExport);
        return atlasExport;
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
}
