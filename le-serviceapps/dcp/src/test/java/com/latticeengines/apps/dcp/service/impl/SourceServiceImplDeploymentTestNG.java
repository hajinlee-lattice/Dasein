package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
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


    }

}
