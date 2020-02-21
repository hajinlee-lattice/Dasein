package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;

public class ProjectServiceImplDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProjectServiceImplDeploymentTestNG.class);

    @Inject
    private ProjectService projectService;

    @BeforeClass(groups = "deployment")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void testCreate() {
        ProjectDetails details = projectService.createProject(mainCustomerSpace, "TestDCPProject",
                Project.ProjectType.Type1, "test@dnb.com");
        Assert.assertNotNull(details);
        Assert.assertNotNull(details.getProjectId());
        Assert.assertNotNull(details.getDropFolderAccess());
        Assert.assertThrows(() -> projectService.createProject(mainCustomerSpace, details.getProjectId(),
                "TestDCPProject", Project.ProjectType.Type1, "test@dnb.com"));

        Assert.assertThrows(() -> projectService.createProject(mainCustomerSpace, "project id",
                "TestDCPProject", Project.ProjectType.Type1, "test@dnb.com"));

        Assert.assertThrows(() -> projectService.createProject(mainCustomerSpace, "Project%id",
                "TestDCPProject", Project.ProjectType.Type1, "test@dnb.com"));

        ProjectDetails details2 = projectService.createProject(mainCustomerSpace, "Project_id",
                "TestDCPProject", Project.ProjectType.Type1, "test@dnb.com");
        Assert.assertNotNull(details2);

    }
}
