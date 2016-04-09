package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.HdfsFileDownloader;
import com.latticeengines.pls.service.impl.HdfsFileDownloaderImpl.DownloadBuilder;

public class HdfsFileDownloadImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static String CONTENTS = "contents";
    private String tenantId;
    private String modelId;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        tenantId = "tenantId";
        modelId = "ms__8e3a9d8c-3bc1-4d21-9c91-0af28afc5c9a-PLSModel";
        String dir = modelingServiceHdfsBaseDir + "/" + tenantId + "/models/ANY_TABLE/" + modelId + "/container_01/";
        HdfsUtils.writeToFile(yarnConfiguration, dir + "file.txt", CONTENTS);
    }

    @AfterClass(groups = { "functional" })
    public void teardown() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, modelingServiceHdfsBaseDir + "/" + tenantId);
    }

    @Test(groups = { "functional" })
    public void testGetFileContentsWithCorrectFilter() throws Exception {
        String filter = ".*.txt";
        HdfsFileDownloader downloader = getDownloader();
        String fileContents = downloader.getFileContents(tenantId, modelId, filter);
        assertEquals(fileContents, CONTENTS);
    }

    @Test(groups = { "functional" })
    public void testGetFileContentsWithWrongFilter() throws Exception {
        String filter = ".*.csv";
        HdfsFileDownloader downloader = getDownloader();
        try {
            downloader.getFileContents(tenantId, modelId, filter);
            Assert.fail("Should have thrown an exception.");
        } catch (Exception e) {
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18023);
        }
    }

    private HdfsFileDownloaderImpl getDownloader() {
        DownloadBuilder builder = new DownloadBuilder();
        builder.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir).setYarnConfiguration(yarnConfiguration);
        return new HdfsFileDownloaderImpl(builder);
    }
}
