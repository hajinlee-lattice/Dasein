package com.latticeengines.apps.dcp.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectRequest;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;

public class ProjectResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private ProjectProxy projectProxy;

    @BeforeClass(groups = {"deployment"})
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = {"deployment"})
    public void testCreateDCPProject() {
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName("createtest");
        projectRequest.setProjectId("createtest");
        projectRequest.setProjectType(Project.ProjectType.Type1);
        ProjectDetails result = projectProxy.createDCPProject(mainTestTenant.getId(), projectRequest,"test@lattice-engines.com");
        assertNotNull(result);
        assertEquals(result.getProjectId(), "createtest");
        projectProxy.deleteProject(mainTestTenant.getId(), "createtest");
    }

    @Test(groups = {"deployment"})
    public void testGetAllDCPProject() {
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setDisplayName("getalltest1");
        projectRequest.setProjectId("getalltest1");
        projectRequest.setProjectType(Project.ProjectType.Type1);
        projectProxy.createDCPProject(mainTestTenant.getId(), projectRequest, "test@lattice-engines.com");
        projectRequest.setDisplayName("getalltest2");
        projectRequest.setProjectId("getalltest2");
        projectProxy.createDCPProject(mainTestTenant.getId(), projectRequest, "test@lattice-engines.com");

        List<ProjectSummary> result = projectProxy.getAllDCPProject(mainTestTenant.getId(), Boolean.FALSE, 0, 20);
        assertNotNull(result);
        Set<String> projectIds = new HashSet<>(Arrays.asList("getalltest1", "getalltest2"));
        for (ProjectSummary project: result) {
            if (project.getProjectId().equals("createtest")) {
                Assert.assertEquals(project.getArchieved(), Boolean.TRUE);
            } else {
                projectIds.remove(project.getProjectId());
            }
        }
        Assert.assertTrue(projectIds.isEmpty());
    }
}
