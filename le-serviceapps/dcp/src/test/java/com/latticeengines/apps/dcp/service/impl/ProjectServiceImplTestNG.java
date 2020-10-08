package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.ProjectUpdateRequest;
import com.latticeengines.domain.exposed.dcp.PurposeOfUse;
import com.latticeengines.domain.exposed.exception.LedpException;

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
        PurposeOfUse purposeOfUse = getPurposeOfUse();
        String description = "Test Project Description " + RandomStringUtils.randomAlphanumeric(3);
        ProjectDetails details = projectService.createProject(customerSpace, displayName, projectType, user,
                purposeOfUse, description);
        Assert.assertEquals(description, details.getProjectDescription());

        description = "Test Project Description " + RandomStringUtils.randomAlphanumeric(3);
        ProjectUpdateRequest request = new ProjectUpdateRequest();
        request.setProjectDescription(description);
        projectService.updateProject(customerSpace, details.getProjectId(), request);

        ProjectInfo projectInfo = projectService.getProjectInfoByProjectId(customerSpace, details.getProjectId());
        Assert.assertNotNull(projectInfo);
        Assert.assertNotNull(projectInfo.getProjectDescription());
        Assert.assertEquals(description, projectInfo.getProjectDescription());

        ProjectDetails projectDetails = projectService.getProjectDetailByProjectId(customerSpace, details.getProjectId(), false, null);
        Assert.assertNotNull(projectDetails);
        Assert.assertNotNull(projectDetails.getProjectDescription());
        Assert.assertNotNull(projectDetails.getPurposeOfUse());
        Assert.assertEquals(description, projectDetails.getProjectDescription());
        Assert.assertEquals(purposeOfUse.getDomain(), projectDetails.getPurposeOfUse().getDomain());
        Assert.assertEquals(purposeOfUse.getRecordType(), projectDetails.getPurposeOfUse().getRecordType());

        Project project = projectService.getProjectByProjectId(customerSpace, details.getProjectId());
        Assert.assertNotNull(project);
        Assert.assertEquals(details.getProjectId(), project.getProjectId());
        Assert.assertEquals(description, project.getProjectDescription());
    }

    @Test(groups = "functional")
    public void testDontCreateWithNotEntitledDataDomain () {
        String customerSpace = "customerSpace" + RandomStringUtils.randomAlphanumeric(4);
        String displayName = "Display Name " + RandomStringUtils.randomAlphanumeric(4);
        Project.ProjectType projectType = Project.ProjectType.Type1;
        String user = "functional_test@dnb.com";
        PurposeOfUse purposeOfUse = getNonEntitledPurposeOfUse();
        String description = "Test Project Description " + RandomStringUtils.randomAlphanumeric(3);
        try {
            ProjectDetails details = projectService.createProject(customerSpace, displayName, projectType, user,
                    purposeOfUse, description);
            Assert.fail("Exception LedpException should have been thrown for invalid DataDomain.");
        }
        catch (LedpException ledpException) {
            String msg = ledpException.getMessage();
            Assert.assertEquals(msg, "Project is not entitled to data domain D&B for Compliance.");
        }
    }

    @Test(groups = "functional")
    public void testDontCreateWithNotEntitledRecordType () {
        String customerSpace = "customerSpace" + RandomStringUtils.randomAlphanumeric(4);
        String displayName = "Display Name " + RandomStringUtils.randomAlphanumeric(4);
        Project.ProjectType projectType = Project.ProjectType.Type1;
        String user = "functional_test@dnb.com";
        PurposeOfUse purposeOfUse = getNonEntitledRecordTypePOU();
        String description = "Test Project Description " + RandomStringUtils.randomAlphanumeric(3);
        try {
            ProjectDetails details = projectService.createProject(customerSpace, displayName, projectType, user,
                    purposeOfUse, description);
            Assert.fail("Exception LedpException should have been thrown for invalid DataDomain.");
        }
        catch (LedpException ledpException) {
            String msg = ledpException.getMessage();
            Assert.assertEquals(msg, "Project is not entitled to record type Analytical Use in data domain D&B for Sales & Marketing.");
        }
    }

    private PurposeOfUse getNonEntitledPurposeOfUse() {
        PurposeOfUse purposeOfUse = new PurposeOfUse();
        purposeOfUse.setDomain(DataDomain.Compliance);
        purposeOfUse.setRecordType(DataRecordType.Domain);
        return purposeOfUse;
    }

    private PurposeOfUse getNonEntitledRecordTypePOU() {
        PurposeOfUse purposeOfUse = new PurposeOfUse();
        purposeOfUse.setDomain(DataDomain.SalesMarketing);
        purposeOfUse.setRecordType(DataRecordType.Analytical);
        return purposeOfUse;
    }
}
