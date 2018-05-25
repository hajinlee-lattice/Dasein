package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class FileUploadServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;

    private InputStream fileInputStream;

    private File dataFile;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();

        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv").getPath());
        fileInputStream = new FileInputStream(dataFile);
    }

    @Test(groups = "deployment")
    public void uploadFile() throws Exception {
        MultiTenantContext.setTenant(mainTestTenant);
        SourceFile sourceFile = fileUploadService.uploadFile("fileUploadServiceImplTestNG.csv", SchemaInterpretation
                        .SalesforceAccount, null, null,
                fileInputStream);
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        Assert.assertNotNull(sourceFile);
        String contents = HdfsUtils
                .getHdfsFileContents(
                        yarnConfiguration, //
                        String.format( //
                                "/Pods/Default/Contracts/%s/Tenants/%s/Spaces/Production/Data/Files/fileUploadServiceImplTestNG.csv", //
                                customerSpace.getContractId(), customerSpace.getTenantId()));
        String expectedContents = FileUtils.readFileToString(dataFile);
        assertEquals(contents, expectedContents);
    }

}
