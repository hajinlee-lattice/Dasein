package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.query.EntityType;

public class SourceServiceImplDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private ProjectService projectService;

    @Inject
    private SourceService sourceService;

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxService dropBoxService;

    @BeforeClass(groups = "deployment")
    public void setup() {
        setupTestEnvironment();
    }


    @Test(groups = "deployment")
    public void testSourceCreate() {
        ProjectDetails details = projectService.createProject(mainCustomerSpace, "TestDCPProject",
                Project.ProjectType.Type1, "test@dnb.com");
        String projectId = details.getProjectId();

        SimpleTemplateMetadata simpleTemplateMetadata = new SimpleTemplateMetadata();
        simpleTemplateMetadata.setEntityType(EntityType.Accounts);
        Source source = sourceService.createSource(mainCustomerSpace, "TestSource", projectId, simpleTemplateMetadata);

        Assert.assertNotNull(source);

        Assert.assertFalse(StringUtils.isBlank(source.getFullPath()));

        DropBoxSummary dropBoxSummary = dropBoxService.getDropBoxSummary();

        Assert.assertNotNull(dropBoxSummary);
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source.getFullPath());

        // create another source under same project
        Source source2 = sourceService.createSource(mainCustomerSpace, "TestSource2", projectId,
                simpleTemplateMetadata);
        Assert.assertNotEquals(source.getSourceId(), source2.getSourceId());
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source2.getFullPath() + "drop/");
        s3Service.objectExist(dropBoxSummary.getBucket(),
                dropBoxService.getDropBoxPrefix() + "/" + source2.getFullPath() + "upload/");

    }

}
