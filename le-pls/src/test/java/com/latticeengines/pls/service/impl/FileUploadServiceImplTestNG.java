package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;

public class FileUploadServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantService tenantService;

    private InputStream fileInputStream;

    private File dataFile;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%sPLSTenant2", contractId));
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }

        tenant1 = new Tenant();
        tenant1.setId("TENANT1");
        tenant1.setName("TENANT1");
        tenantEntityMgr.create(tenant1);
        setupSecurityContext(tenant1);

        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        Tenant tenant1 = tenantService.findByTenantId("TENANT1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    @Test(groups = "functional")
    public void uploadFile() throws Exception {
        fileUploadService.uploadFile("fileUploadServiceImplTestNG.csv", SchemaInterpretation.SalesforceAccount,
                fileInputStream);

        String contents = HdfsUtils
                .getHdfsFileContents(
                        yarnConfiguration, //
                        String.format( //
                                "/Pods/Default/Contracts/%s/Tenants/%s/Spaces/Production/Data/Files/fileUploadServiceImplTestNG.csv", //
                                "TENANT1", "TENANT1"));
        String expectedContents = FileUtils.readFileToString(dataFile);
        assertEquals(contents, expectedContents);
    }

    @Test(groups = "functional")
    public void uploadFileWithMissingRequiredFields() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_missing_required_fields.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        try {
            fileUploadService.uploadFile("fileUploadServiceImplTestNG.csv", SchemaInterpretation.SalesforceAccount,
                    fileInputStream);
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertTrue(e.getMessage().contains(InterfaceName.Id.name()));
            assertTrue(e.getMessage().contains(InterfaceName.Website.name()));
            assertTrue(e.getMessage().contains(InterfaceName.Event.name()));
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18087);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithEmptyHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_empty_header.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        try {
            fileUploadService.uploadFile("fileUploadServiceImplTestNG.csv", SchemaInterpretation.SalesforceAccount,
                    fileInputStream);
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18096);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithUnexpectedCharacterInHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_unexpected_character_in_header.csv")
                .getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        try {
            fileUploadService.uploadFile("fileUploadServiceImplTestNG.csv", SchemaInterpretation.SalesforceAccount,
                    fileInputStream);
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18095);
        }
    }
}
