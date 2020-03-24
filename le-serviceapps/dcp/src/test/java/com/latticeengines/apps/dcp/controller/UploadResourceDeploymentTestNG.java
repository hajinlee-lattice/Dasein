package com.latticeengines.apps.dcp.controller;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;

public class UploadResourceDeploymentTestNG extends DCPDeploymentTestNGBase {
    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;

    @BeforeClass(groups = {"deployment"})
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = {"deployment"})
    public void testCRUD() {
        ProjectDetails details = projectService.createProject(mainCustomerSpace, "TestDCPProject",
                Project.ProjectType.Type1, "test@dnb.com");
        String projectId = details.getProjectId();

        SimpleTemplateMetadata simpleTemplateMetadata = new SimpleTemplateMetadata();
        simpleTemplateMetadata.setEntityType(EntityType.Accounts);
        Source source = sourceService.createSource(mainCustomerSpace, "TestSource", projectId, simpleTemplateMetadata);

        UploadConfig config = new UploadConfig();
        config.setDropFilePath("/drop");
        config.setErrorFilePath("/error");
        Upload upload = uploadProxy.createUpload(mainCustomerSpace, source.getSourceId(), config);
        Assert.assertEquals(upload.getStatus(), Upload.Status.NEW);
        UploadConfig returnedConfig = upload.getUploadConfig();
        Assert.assertEquals(returnedConfig.getDropFilePath(), "/drop");
        Assert.assertEquals(returnedConfig.getErrorFilePath(), "/error");
        Assert.assertNull(returnedConfig.getProcessedFilePath());
        Assert.assertNull(returnedConfig.getUploadFilePath());


        // update config
        config.setProcessedFilePath("/processed");
        config.setUploadFilePath("/uploaded");
        uploadProxy.updateConfig(mainCustomerSpace, upload.getPid(), config);
        List<Upload> uploads = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), null);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        Upload retrievedUpload = uploads.get(0);
        UploadConfig retrievedConfig = retrievedUpload.getUploadConfig();
        Assert.assertEquals(retrievedConfig.getProcessedFilePath(), "/processed");
        Assert.assertEquals(retrievedConfig.getUploadFilePath(), "/uploaded");
        Assert.assertEquals(retrievedConfig.getDropFilePath(), "/drop");
        Assert.assertEquals(retrievedConfig.getErrorFilePath(), "/error");

        uploadProxy.updateStatus(mainCustomerSpace, upload.getPid(), Upload.Status.MATCH_STARTED);
        uploads = uploadProxy.getUploads(mainCustomerSpace, source.getSourceId(), Upload.Status.MATCH_STARTED);
        Assert.assertNotNull(uploads);
        Assert.assertEquals(uploads.size(), 1);
        retrievedUpload = uploads.get(0);
        Assert.assertEquals(retrievedUpload.getStatus(), Upload.Status.MATCH_STARTED);





    }
}
