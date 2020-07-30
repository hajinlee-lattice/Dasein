package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.entitymgr.ProjectEntityMgr;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;

public class ProjectEntityMgrImplTestNG extends DCPFunctionalTestNGBase {

    private static final String SYSTEM_NAME_PATTERN = "ProjectSystem_%s";

    @Inject
    private ProjectEntityMgr projectEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testList() {
        List<String> projectIdList = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            projectIdList.add("Project_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        for (int i = 0; i < projectIdList.size(); i++) {
            S3ImportSystem system = getS3ImportSystem(projectIdList.get(i), i + 1);
            system.setPid(createImportSystem(mainTestTenant.getPid(), system.getName(), system.getPriority()));
            projectEntityMgr.create(generateProjectObject(projectIdList.get(i), system));
        }
        SleepUtils.sleep(1000);
        List<ProjectInfo> allProjects = new ArrayList<>();
        List<ProjectInfo> projectInfoList;
        int pageSize = 3;
        int pageIndex = 0;
        do {
            Sort sort = Sort.by(Sort.Direction.DESC, "updated");
            PageRequest pageRequest = PageRequest.of(pageIndex, pageSize, sort);
            projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest);
            if (pageIndex < 6) {
                Assert.assertEquals(projectInfoList.size(), pageSize);
            } else {
                Assert.assertEquals(projectInfoList.size(), 20 % pageSize);
            }
            allProjects.addAll(projectInfoList);
            pageIndex++;
        } while (CollectionUtils.size(projectInfoList) == pageSize);

        Sort sort = Sort.by(Sort.Direction.DESC, "pid");
        PageRequest pageRequest = PageRequest.of(pageIndex, pageSize, sort);
        projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest);
        Assert.assertEquals(CollectionUtils.size(projectInfoList), 0);

        long allProjectsCount = projectEntityMgr.countAllProjects();

        Assert.assertEquals(CollectionUtils.size(allProjects), allProjectsCount);
        for (int i = 0; i < allProjects.size(); i++) {
            Assert.assertEquals(allProjects.get(i).getProjectId(), projectIdList.get(19 - i));
        }

    }

    private Project generateProjectObject(String projectId, S3ImportSystem system) {
        Project project = new Project();
        project.setCreatedBy("testdcp@dnb.com");
        project.setProjectDisplayName(projectId);
        project.setProjectId(projectId);
        project.setTenant(MultiTenantContext.getTenant());
        project.setDeleted(Boolean.FALSE);
        project.setProjectType(Project.ProjectType.Type1);
        project.setRootPath(String.format("Projects/%s/", projectId));
        project.setS3ImportSystem(system);
        return project;
    }

    private S3ImportSystem getS3ImportSystem(String projectId, int priority) {
        S3ImportSystem system = new S3ImportSystem();
        system.setTenant(mainTestTenant);
        String systemName = String.format(SYSTEM_NAME_PATTERN, projectId);
        system.setName(systemName);
        system.setDisplayName(systemName);
        system.setSystemType(S3ImportSystem.SystemType.DCP);
        system.setPriority(priority);
        return system;
    }


    private Long createImportSystem(long tenantPid, String systemName, int priority) {
        String sql = "INSERT INTO `ATLAS_S3_IMPORT_SYSTEM` ";
        sql += "(`TENANT_ID`, `FK_TENANT_ID`, `SYSTEM_TYPE`, `NAME`, `PRIORITY`, `DISPLAY_NAME`) VALUES ";
        sql += String.format("(%d, %d, 'DCP', '%s', '%d', '%s')", tenantPid, tenantPid, systemName, priority, systemName);
        jdbcTemplate.execute(sql);
        SleepUtils.sleep(1000);
        sql = "SELECT `PID` FROM `ATLAS_S3_IMPORT_SYSTEM` WHERE ";
        sql += String.format("`TENANT_ID` = %d", tenantPid);
        sql += String.format(" AND `NAME` = '%s'", systemName);
        return jdbcTemplate.queryForObject(sql, Long.class);
    }
}
