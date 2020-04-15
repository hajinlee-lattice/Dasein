package com.latticeengines.pls.entitymanager.impl;

import java.util.UUID;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.dcp.UploadFileDownloadConfig;
import com.latticeengines.domain.exposed.pls.FileDownload;
import com.latticeengines.pls.entitymanager.FileDownloadEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class FileDownloadEntityMgrImplTestNG extends PlsFunctionalTestNGBase {


    @Inject
    private FileDownloadEntityMgr fileDownloadEntityMgr;

    @Test(groups = "functional")
    public void testFileDownload() {
        FileDownload download = new FileDownload();
        String token = HashUtils.getMD5CheckSum(UUID.randomUUID().toString());
        download.setTenant(mainTestTenant);
        download.setToken(token);
        download.setTtl(10);
        download.setCreation(System.currentTimeMillis());
        UploadFileDownloadConfig config = new UploadFileDownloadConfig();
        config.setUploadId(String.valueOf(1l));
        download.setFileDownloadConfig(config);
        fileDownloadEntityMgr.create(download);
        SleepUtils.sleep(300);

        FileDownload retrieved = fileDownloadEntityMgr.findByToken(token);
        Assert.assertNotNull(retrieved);

        fileDownloadEntityMgr.delete(retrieved);
        SleepUtils.sleep(500);

        retrieved = fileDownloadEntityMgr.findByToken(token);
        Assert.assertNull(retrieved);

    }


}
