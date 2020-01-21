package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.security.exposed.service.TenantService;
public class FileUploadServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Inject
    private FileUploadService fileUploadService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
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
        String filePath = String.format("%s/fileUploadServiceImplTestNG.csv",
                PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), customerSpace).toString());
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, filePath);
        String expectedContents = FileUtils.readFileToString(dataFile);
        assertEquals(contents, expectedContents);
    }

}
