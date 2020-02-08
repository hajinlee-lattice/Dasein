package com.latticeengines.apps.dcp.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.DCPProjectService;
import com.latticeengines.apps.dcp.testframework.DCPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;

public class DCPProjectServiceImplDeploymentTestNG extends DCPDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DCPProjectServiceImplDeploymentTestNG.class);

    @Inject
    private DCPProjectService dcpProjectService;

    @BeforeClass(groups = "deployment")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void testCreate() {
        DCPProjectDetails details = dcpProjectService.createDCPProject(mainCustomerSpace, "TestDCPProject",
                DCPProject.ProjectType.Type1, "test@dnb.com");
        Assert.assertNotNull(details);
        Assert.assertNotNull(details.getProjectId());
        Assert.assertNotNull(details.getDropFolderAccess());
        Assert.assertThrows(() -> dcpProjectService.createDCPProject(mainCustomerSpace, details.getProjectId(),
                "TestDCPProject", DCPProject.ProjectType.Type1, "test@dnb.com"));

        Assert.assertThrows(() -> dcpProjectService.createDCPProject(mainCustomerSpace, "project id",
                "TestDCPProject", DCPProject.ProjectType.Type1, "test@dnb.com"));

        Assert.assertThrows(() -> dcpProjectService.createDCPProject(mainCustomerSpace, "Project%id",
                "TestDCPProject", DCPProject.ProjectType.Type1, "test@dnb.com"));

        DCPProjectDetails details2 = dcpProjectService.createDCPProject(mainCustomerSpace, "Project_id",
                "TestDCPProject", DCPProject.ProjectType.Type1, "test@dnb.com");
        Assert.assertNotNull(details2);

    }
}
