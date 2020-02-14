package com.latticeengines.apps.dcp.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;

public class ProjectResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private ProjectProxy projectProxy;

    @BeforeClass(groups = {"deployment"})
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = {"deployment"})
    public void testCreateDCPProject() throws IOException {
        ProjectDetails result = projectProxy.createDCPProject(mainTestTenant.getId(), "createtest", "createtest",
                Project.ProjectType.Type1, "test@lattice-engines.com");
        assertNotNull(result);
        assertEquals(result.getProjectId(), "createtest");
        projectProxy.deleteProject(mainTestTenant.getId(), "createtest");
    }

    @Test(groups = {"deployment"})
    public void testGetAllDCPProject() throws IOException {
        projectProxy.createDCPProject(mainTestTenant.getId(), "getalltest1", "getalltest1",
                Project.ProjectType.Type1, "test@lattice-engines.com");
        projectProxy.createDCPProject(mainTestTenant.getId(), "getalltest2", "getalltest2",
                Project.ProjectType.Type2, "test@lattice-engines.com");

        List<Project> result = projectProxy.getAllDCPProject(mainTestTenant.getId());
        assertNotNull(result);
        assertEquals(result.get(0).getProjectId(), "getalltest1");
        assertEquals(result.get(1).getProjectId(), "getalltest2");
    }
}
