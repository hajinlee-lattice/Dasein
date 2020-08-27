package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;

public class ProjectServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    ProjectService projectService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();

    }

    @Test(groups = "functional")
    public void testCreateAndFindProject() {
        String customerSpace = "customerSpace" + RandomStringUtils.randomAlphanumeric(4);
        String displayName = "Display Name " + RandomStringUtils.randomAlphanumeric(4);
        Project.ProjectType projectType = Project.ProjectType.Type1;
        String user = "functional_test@dnb.com";

        ProjectDetails details = projectService.createProject(customerSpace, displayName, projectType, user);
        Assert.assertNotNull(details);

        String description = "Test Project Description " + RandomStringUtils.randomAlphanumeric(3);
        projectService.updateDescription(customerSpace, details.getProjectId(), description);

        ProjectInfo projectInfo = projectService.getProjectInfoByProjectId(customerSpace, details.getProjectId());
        Assert.assertNotNull(projectInfo);
        Assert.assertNotNull(projectInfo.getProjectDescription());
        Assert.assertEquals(description, projectInfo.getProjectDescription());

        ProjectDetails projectDetails = projectService.getProjectDetailByProjectId(customerSpace, details.getProjectId(), false, null);
        Assert.assertNotNull(projectDetails);
        Assert.assertNotNull(projectDetails.getProjectDescription());
        Assert.assertEquals(description, projectDetails.getProjectDescription());

        Project project = projectService.getProjectByProjectId(customerSpace, details.getProjectId());
        Assert.assertNotNull(project);
        Assert.assertEquals(details.getProjectId(), project.getProjectId());
        Assert.assertEquals(description, project.getProjectDescription());
    }

}
