package com.latticeengines.app.entitymanager.impl;

import java.util.UUID;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.FileDownloadEntityMgr;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.dcp.UploadFileDownloadConfig;
import com.latticeengines.domain.exposed.pls.FileDownload;

public class FileDownloadEntityMgrImplTestNG extends AppFunctionalTestNGBase {

    @Inject
    private FileDownloadEntityMgr fileDownloadEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithOneTenant();
    }

    @Test(groups = "functional")
    public void testFileDownload() {
        FileDownload download = new FileDownload();
        String token = HashUtils.getMD5CheckSum(UUID.randomUUID().toString());
        download.setTenant(mainTestTenant);
        download.setToken(token);
        download.setTtl(10);
        download.setCreation(System.currentTimeMillis());
        UploadFileDownloadConfig config = new UploadFileDownloadConfig();
        config.setUploadId(String.valueOf(1L));
        download.setFileDownloadConfig(config);
        fileDownloadEntityMgr.create(download);
        SleepUtils.sleep(300);

        FileDownload retrieved = fileDownloadEntityMgr.getByToken(token);
        Assert.assertNotNull(retrieved);
    }


}
