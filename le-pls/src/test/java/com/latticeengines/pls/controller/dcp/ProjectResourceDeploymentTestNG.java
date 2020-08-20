package com.latticeengines.pls.controller.dcp;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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

        RetryTemplate retry = RetryUtils.getRetryTemplate(5,
                Collections.singleton(AssertionError.class), null);
        retry.execute(ctx -> {
            List<ProjectSummary> allProjects = testProjectProxy.getAllProjects();
            Assert.assertEquals(allProjects.size(), 4);
            allProjects.forEach(project -> Assert.assertEquals(project.getArchieved(), Boolean.TRUE));
            return true;
        });
    }

    @Test(groups = "deployment")
    public void testGetAllDCPProjectWithTeamRestriction() {
        List<ProjectSummary> projectList = testProjectProxy.getAllProjects();
        Assert.assertTrue(CollectionUtils.isNotEmpty(projectList));
        Assert.assertEquals(projectList.size(), 4);
        switchToExternalAdmin();
        String url = getRestAPIHostPort() + "/pls/projects/list";
        projectList = restTemplate.getForObject(url, List.class);
        Assert.assertEquals(projectList.size(), 0);

        switchToSuperAdmin();
        projectList = testProjectProxy.getAllProjects();
        Assert.assertEquals(projectList.size(), 4);

        ProjectDetails project = testProjectProxy.getProjectByProjectId(projectList.get(0).getProjectId());
        GlobalTeamData teamData = new GlobalTeamData();
        teamData.setTeamName(project.getProjectId());
        String externalAdminUser = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.EXTERNAL_ADMIN);
        String superAdminUser = TestFrameworkUtils.usernameForAccessLevel(AccessLevel.SUPER_ADMIN);
        teamData.setTeamMembers(Sets.newHashSet(externalAdminUser, superAdminUser));
        url = getRestAPIHostPort() + "/pls/teams/teamId/" + project.getTeamId();
        restTemplate.put(url, teamData);

        cleanupSession(AccessLevel.EXTERNAL_ADMIN);
        switchToExternalAdmin();
        url = getRestAPIHostPort() + "/pls/projects/list";
        projectList = restTemplate.getForObject(url, List.class);
        Assert.assertEquals(projectList.size(), 1);
    }
}
