package com.latticeengines.pls.service.impl.dcp;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.datanucleus.util.StringUtils;
import org.joda.time.DateTime;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.dcp.SourceFileUploadService;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;

public class SourceFileUploadServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private SourceFileUploadService sourceFileUploadService;

    @Inject
    private Configuration yarnConfiguration;

    private String customerSpace;


    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testUploadFile() throws IOException {
        MultipartFile multipartFile = new MockMultipartFile("TestFileName.txt",
                "Test Content = Hello World!".getBytes());
        SourceFileInfo sourceFileInfo = sourceFileUploadService.uploadFile("file_" + DateTime.now().getMillis() +
                ".txt", "TestFileName.txt", false, null, multipartFile);

        Assert.assertNotNull(sourceFileInfo);
        Assert.assertFalse(StringUtils.isEmpty(sourceFileInfo.getName()));
        Assert.assertEquals(sourceFileInfo.getDisplayName(), "TestFileName.txt");

        SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, sourceFileInfo.getName());
        Assert.assertNotNull(sourceFile);
        Assert.assertFalse(StringUtils.isEmpty(sourceFile.getPath()));
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, sourceFile.getPath()));
        Assert.assertEquals(HdfsUtils.getHdfsFileContents(yarnConfiguration, sourceFile.getPath()),
                "Test Content = Hello World!");
    }
}
