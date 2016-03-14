package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.SourceFileService;

public class FileUploadServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private Configuration yarnConfiguration;

    private InputStream fileInputStream;

    private File dataFile;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, String.format("/Pods/Default/Contracts/%sPLSTenant2", contractId));
        setUpMarketoEloquaTestEnvironment();
        switchToSuperAdmin();
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv").getPath());
        fileInputStream = new FileInputStream(dataFile);
    }

    @Test(groups = "functional")
    public void uploadFile() throws Exception {
        fileUploadService.uploadFile("fileUploadServiceImplTestNG.csv", SchemaInterpretation.SalesforceAccount,
                fileInputStream);

        String contents = HdfsUtils
                .getHdfsFileContents(
                        yarnConfiguration, //
                        String.format( //
                                "/Pods/Default/Contracts/%sPLSTenant2/Tenants/%sPLSTenant2/Spaces/Production/Data/Files/file1.csv", //
                                contractId, contractId));
        String expectedContents = FileUtils.readFileToString(dataFile);
        assertEquals(contents, expectedContents);
    }
}
