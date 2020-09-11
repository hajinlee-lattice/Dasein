package com.latticeengines.pls.controller.dcp;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectSummary;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.proxy.pls.TestProjectProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class ProjectResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProjectResourceDeploymentTestNG.class);

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

    // Test creating a project with a provided ID and making sure that deleting the project moves it to archived
    // state.
    @Test(groups = "deployment")
    public void testCreateDCPProjectWithProjectId() {
        ProjectDetails projectDetail = testProjectProxy.createProjectWithProjectId(DISPLAY_NAME, PROJECT_ID,
                Project.ProjectType.Type1);
        assertNotNull(projectDetail);
        assertEquals(projectDetail.getProjectId(), PROJECT_ID);
    }

    // Test creating a project without a provided ID and making sure that deleting the project moves it to archived
    // state.
    @Test(groups = "deployment")
    public void testCreateDCPProjectWithOutProjectId() {
        ProjectDetails projectDetail = testProjectProxy.createProjectWithOutProjectId(DISPLAY_NAME,
                Project.ProjectType.Type1);
        assertNotNull(projectDetail);
        assertEquals(projectDetail.getProjectDisplayName(), DISPLAY_NAME);
        testProjectProxy.deleteProject(projectDetail.getProjectId());

        projectDetail = testProjectProxy.getProjectByProjectId(projectDetail.getProjectId());
        Assert.assertNotNull(projectDetail);
        Assert.assertEquals(projectDetail.getDeleted(), Boolean.TRUE);
    }

    // Test getting the list of projects with and without archived projects.
    @Test(groups = "deployment", dependsOnMethods = {"testCreateDCPProjectWithProjectId",
            "testCreateDCPProjectWithOutProjectId"})
    public void testGetAllDCPProject() {
        ProjectDetails projectDetail1 = testProjectProxy.createProjectWithOutProjectId(DISPLAY_NAME,
                Project.ProjectType.Type1);
        assertNotNull(projectDetail1);
        ProjectDetails projectDetail2 = testProjectProxy.createProjectWithOutProjectId(DISPLAY_NAME,
                Project.ProjectType.Type1);
        assertNotNull(projectDetail2);

        // Check only non-archived projects.
        List<ProjectSummary> projectList = testProjectProxy.getAllProjects(false, false);
        Assert.assertTrue(CollectionUtils.isNotEmpty(projectList));
        Assert.assertEquals(projectList.size(), 3);
        projectList.forEach(project -> {
            Assert.assertEquals(project.getArchived(), Boolean.FALSE);
            Assert.assertNotNull(project.getPurposeOfUse());
        });

        // Check all projects.  There should be 4.
        projectList = testProjectProxy.getAllProjects(false, true);
        Assert.assertTrue(CollectionUtils.isNotEmpty(projectList));
        Assert.assertEquals(projectList.size(), 4);


        // Now delete one of the projects just created.
        testProjectProxy.deleteProject(projectDetail1.getProjectId());

        RetryTemplate retry = RetryUtils.getRetryTemplate(5,
                Collections.singleton(AssertionError.class), null);
        retry.execute(ctx -> {
            // Check the non-archived projects.  There should be one less.
            List<ProjectSummary> projectList2 = testProjectProxy.getAllProjects(false, false);
            Assert.assertTrue(CollectionUtils.isNotEmpty(projectList2));
            Assert.assertEquals(projectList2.size(), 2);
            projectList2.forEach(project -> Assert.assertEquals(project.getArchived(), Boolean.FALSE));

            // Check all the projects.  There should still be 4.
            projectList2 = testProjectProxy.getAllProjects(false, true);
            Assert.assertTrue(CollectionUtils.isNotEmpty(projectList2));
            Assert.assertEquals(projectList2.size(), 4);
            return true;
        });
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetAllDCPProject")
    public void testGetAllDCPProjectWithTeamRestriction() {
        // Check the projects viewable by a Super Admin.  All created projects should be visible.
        switchToSuperAdmin();
        List<ProjectSummary> projectList = testProjectProxy.getAllProjects(false, true);
        Assert.assertTrue(CollectionUtils.isNotEmpty(projectList));
        Assert.assertEquals(projectList.size(), 4);

        // Switch to an External Admin.  They cannot see the Super Admin's projects.
        switchToExternalAdmin();
        projectList = testProjectProxy.getAllProjects(false, true);
        Assert.assertTrue(CollectionUtils.isEmpty(projectList));

        // Add a project as an External Admin.
        ProjectDetails projectDetail3 = testProjectProxy.createProjectWithOutProjectId(DISPLAY_NAME,
                Project.ProjectType.Type1);
        assertNotNull(projectDetail3);

        RetryTemplate retry = RetryUtils.getRetryTemplate(5,
                Collections.singleton(AssertionError.class), null);
        retry.execute(ctx -> {
            // Check all projects,
            List<ProjectSummary> projectList2 = testProjectProxy.getAllProjects(false, true);
            Assert.assertTrue(CollectionUtils.isNotEmpty(projectList2));
            Assert.assertEquals(projectList2.size(), 1);
            return true;
        });

        // Switch back to Super Admin.  They should now see 5 projects.
        switchToSuperAdmin();
        projectList = testProjectProxy.getAllProjects(false, true);
        Assert.assertTrue(CollectionUtils.isNotEmpty(projectList));
        Assert.assertEquals(projectList.size(), 5);

        // Create a team that includes the External Admin and System Admin, so that the External Admin can see an
        // additional project.
        ProjectDetails project = testProjectProxy.getProjectByProjectId(PROJECT_ID);
        GlobalTeamData teamData = new GlobalTeamData();
        teamData.setTeamName(project.getProjectId());
        String externalAdminUser = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        String superAdminUser = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
        teamData.setTeamMembers(Sets.newHashSet(externalAdminUser, superAdminUser));
        String url = getRestAPIHostPort() + "/pls/teams/teamId/" + project.getTeamId();
        restTemplate.put(url, teamData);

        cleanupSession(AccessLevel.EXTERNAL_ADMIN);
        switchToExternalAdmin();
        projectList = testProjectProxy.getAllProjects(false, true);
        Assert.assertTrue(CollectionUtils.isNotEmpty(projectList));
        // The External Admin should now be able to see 2 projects.
        Assert.assertEquals(projectList.size(), 2);
    }
}
