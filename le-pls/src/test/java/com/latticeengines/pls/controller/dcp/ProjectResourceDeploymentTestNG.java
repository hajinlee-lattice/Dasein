package com.latticeengines.pls.controller.dcp;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.TestProjectProxy;

public class ProjectResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final String DISPLAY_NAME = "testProject";
    private static final String PROJECT_ID = "testProject";

    @Inject
    TestProjectProxy testProjectProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        attachProtectedProxy(testProjectProxy);
    }

    @Test(groups = "deployment")
    public void testCreateDCPProjectWithProjectId() {
        ProjectDetails projectDetail = testProjectProxy.createProjectWithProjectId(DISPLAY_NAME, PROJECT_ID, Project.ProjectType.Type1);
        assertNotNull(projectDetail);
        assertEquals(projectDetail.getProjectId(), PROJECT_ID);
        testProjectProxy.deleteProject(PROJECT_ID);
    }

    @Test(groups = "deployment")
    public void testCreateDCPProjectWithOutProjectId() {
        ProjectDetails projectDetail = testProjectProxy.createProjectWithOutProjectId(DISPLAY_NAME, Project.ProjectType.Type1);
        assertNotNull(projectDetail);
        assertEquals(projectDetail.getProjectDisplayName(), DISPLAY_NAME);
        testProjectProxy.deleteProject(projectDetail.getProjectId());

        projectDetail = testProjectProxy.getProjectByProjectId(projectDetail.getProjectId());
        Assert.assertNotNull(projectDetail);
        Assert.assertEquals(projectDetail.getDeleted(), Boolean.TRUE);
    }

    @Test(groups = "deployment")
    public void testGetAllDCPProject() {
        ProjectDetails projectDetail1 = testProjectProxy.createProjectWithOutProjectId(DISPLAY_NAME, Project.ProjectType.Type1);
        assertNotNull(projectDetail1);
        ProjectDetails projectDetail2 = testProjectProxy.createProjectWithOutProjectId(DISPLAY_NAME, Project.ProjectType.Type1);
        assertNotNull(projectDetail2);

        List<ProjectSummary> projectList = testProjectProxy.getAllProjects();
        Assert.assertTrue(CollectionUtils.isNotEmpty(projectList));
        Assert.assertEquals(projectList.size(), 4);

        testProjectProxy.deleteProject(projectDetail1.getProjectId());
        testProjectProxy.deleteProject(projectDetail2.getProjectId());
        SleepUtils.sleep(1000L);

        projectList = testProjectProxy.getAllProjects();
        Assert.assertEquals(projectList.size(), 4);
        projectList.forEach(project -> Assert.assertEquals(project.getArchieved(), Boolean.TRUE));
    }
}
