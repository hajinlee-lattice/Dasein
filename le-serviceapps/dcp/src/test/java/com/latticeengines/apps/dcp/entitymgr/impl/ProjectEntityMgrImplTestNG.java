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
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;

public class ProjectEntityMgrImplTestNG extends DCPFunctionalTestNGBase {

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
        for (String s : projectIdList) {
            SleepUtils.sleep(1000L);
            projectEntityMgr.create(generateProjectObject(s));
        }
        SleepUtils.sleep(1000);
        List<ProjectInfo> allProjects = new ArrayList<>();
        List<ProjectInfo> projectInfoList;
        int pageSize = 3;
        int pageIndex = 0;
        do {
            Sort sort = Sort.by(Sort.Direction.DESC, "updated");
            PageRequest pageRequest = PageRequest.of(pageIndex, pageSize, sort);
            projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest, Boolean.FALSE);
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
        projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest, Boolean.FALSE);
        Assert.assertEquals(CollectionUtils.size(projectInfoList), 0);

        long allProjectsCount = projectEntityMgr.countAllProjects();

        Assert.assertEquals(CollectionUtils.size(allProjects), allProjectsCount);
        for (int i = 0; i < allProjects.size(); i++) {
            Assert.assertEquals(allProjects.get(i).getProjectId(), projectIdList.get(19 - i));
        }

    }

    private Project generateProjectObject(String projectId) {
        Project project = new Project();
        project.setCreatedBy("testdcp@dnb.com");
        project.setProjectDisplayName(projectId);
        project.setProjectId(projectId);
        project.setTenant(MultiTenantContext.getTenant());
        project.setDeleted(Boolean.FALSE);
        project.setProjectType(Project.ProjectType.Type1);
        project.setRootPath(String.format("Projects/%s/", projectId));
        return project;
    }
}
