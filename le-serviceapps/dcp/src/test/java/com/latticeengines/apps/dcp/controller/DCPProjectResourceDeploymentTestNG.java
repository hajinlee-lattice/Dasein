package com.latticeengines.apps.dcp.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;
import com.latticeengines.proxy.exposed.cdl.DCPProxy;

public class DCPProjectResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private DCPProxy dcpProxy;

    @BeforeClass(groups = {"deployment"})
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = {"deployment"})
    public void testCreateDCPProject() throws IOException {
        DCPProjectDetails result = dcpProxy.createDCPProject(mainTestTenant.getId(), "createtest", "createtest",
                DCPProject.ProjectType.Type1, "test@lattice-engines.com");
        assertNotNull(result);
        assertEquals(result.getProjectId(), "createtest");
        dcpProxy.deleteProject(mainTestTenant.getId(), "createtest");
    }

    @Test(groups = {"deployment"})
    public void testGetAllDCPProject() throws IOException {
        dcpProxy.createDCPProject(mainTestTenant.getId(), "getalltest1", "getalltest1",
                DCPProject.ProjectType.Type1, "test@lattice-engines.com");
        dcpProxy.createDCPProject(mainTestTenant.getId(), "getalltest2", "getalltest2",
                DCPProject.ProjectType.Type2, "test@lattice-engines.com");

        List<DCPProject> result = dcpProxy.getAllDCPProject(mainTestTenant.getId());
        assertNotNull(result);
        assertEquals(result.get(0).getProjectId(), "getalltest1");
        assertEquals(result.get(1).getProjectId(), "getalltest2");
    }
}
