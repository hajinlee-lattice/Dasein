package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dcp.SourceFileInfo;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.FileUploadProxy;

public class FileUploadResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";
    private static final String fileName = "file1.csv";

    @Inject
    FileUploadProxy fileUploadProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(fileUploadProxy);
    }

    @Test(groups = "deployment")
    public void testUploadFile() throws Exception {
        Resource csvResource = new ClassPathResource(PATH,
                Thread.currentThread().getContextClassLoader());
        SourceFileInfo testSourceFile = fileUploadProxy.uploadFile(fileName, csvResource);
        Assert.assertNotNull(testSourceFile);
    }
}
