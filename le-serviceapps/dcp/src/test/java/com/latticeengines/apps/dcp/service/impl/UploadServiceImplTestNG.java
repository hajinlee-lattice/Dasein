package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;

public class UploadServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private UploadService uploadService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCreateGetUpdate() {
        String sourceId1 = "sourceId1";
        String sourceId2 = "sourceId2";
        Upload upload1 = uploadService.createUpload(mainCustomerSpace, sourceId1, null);
        Upload upload2 = uploadService.createUpload(mainCustomerSpace, sourceId1, null);
        Upload upload3 = uploadService.createUpload(mainCustomerSpace, sourceId2, null);
        Assert.assertNotNull(upload1.getPid());
        Assert.assertNotNull(upload2.getPid());
        Assert.assertNotNull(upload3.getPid());
        List<Upload> uploads = uploadService.getUploads(mainCustomerSpace, sourceId1);
        Assert.assertTrue(CollectionUtils.isNotEmpty(uploads));
        Assert.assertEquals(uploads.size(), 2);
        uploads.forEach(upload -> {
            Assert.assertNull(upload.getUploadConfig());
            Assert.assertEquals(upload.getStatus(), Upload.Status.NEW);
        });
        UploadConfig uploadConfig = new UploadConfig();
        uploadConfig.setDropFilePath("DummyPath");
        uploadService.updateUploadConfig(mainCustomerSpace, upload1.getPid(), uploadConfig);
        uploadService.updateUploadStatus(mainCustomerSpace, upload2.getPid(), Upload.Status.IMPORT_STARTED);
        uploads = uploadService.getUploads(mainCustomerSpace, sourceId1);
        Assert.assertEquals(uploads.size(), 2);
        uploads.forEach(upload -> {
            if (upload.getPid().longValue() == upload1.getPid().longValue()) {
                Assert.assertNotNull(upload.getUploadConfig());
            } else {
                Assert.assertEquals(upload.getStatus(), Upload.Status.IMPORT_STARTED);
            }
        });
    }
}
