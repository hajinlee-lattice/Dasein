package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.mapreduce.v2.app.speculate.TaskRuntimeEstimator;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.entitymgr.ProjectEntityMgr;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
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
            projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest, Boolean.FALSE /* false == include archived */);
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

        // Now really delete all projects so it doesn't interfere with the test below
        for (String projectId : projectIdList) {
            Project project = projectEntityMgr.findByProjectId(projectId);
            project.setDeleted(Boolean.TRUE);
            projectEntityMgr.delete(project);
        }
    }

    @Test(groups = "functional")
    public void testListWithArchivedProjects() {

        Sort sort = Sort.by(Sort.Direction.DESC, "updated");
        PageRequest page = PageRequest.of(0,50, sort);
        List<String> projectIdList = new ArrayList<>();
        int totalCount = 20;
        // create some projects
        for (int i = 0; i < totalCount; i++) {
            projectIdList.add("Project_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        for (String s : projectIdList) {
            SleepUtils.sleep(1000L);
            projectEntityMgr.create(generateProjectObject(s));
        }
        SleepUtils.sleep(1000);
        // Check that all 20 are created
        Assert.assertEquals(projectEntityMgr.findAllProjectInfo(page, Boolean.FALSE).size(),totalCount);
        // Now, Mark 4 projects as deleted
        List<Integer> rmIdx = Arrays.asList(2,4,7,12); // delete the projects at these positions
        for (Integer idx : rmIdx) {
            Project project = projectEntityMgr.findByProjectId(projectIdList.get(idx));
            project.setDeleted(Boolean.TRUE);
            projectEntityMgr.update(project);
        }
        // activeCount is the count of not deleted projects (also know as not archived projects)
        int activeCount = totalCount - rmIdx.size();

        Assert.assertEquals((long)projectEntityMgr.countAllProjects(), totalCount);
        Assert.assertEquals((long)projectEntityMgr.countAllActiveProjects(), activeCount);

        // check that finding all projects including archived returns the total count
        Assert.assertEquals(projectEntityMgr.findAllProjectInfo(page, Boolean.TRUE).size(), totalCount);

        // check that finding all projects not including archived returns the active count.
        Assert.assertEquals(filterArchived(projectEntityMgr.findAllProjectInfo(page, Boolean.FALSE)).size(), activeCount);

        List<ProjectInfo> allActiveProjects = new ArrayList<>();
        List<ProjectInfo> projectInfoList;
        int pageSize = 3;
        int pageIndex = 0;
        do {
            PageRequest pageRequest = PageRequest.of(pageIndex, pageSize, sort);
            projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest, Boolean.FALSE /* false == dont include archived */);
            if (pageIndex < Math.floor((float)activeCount / pageSize)) {
                // Did we get 1 page sized amount of projects?
                Assert.assertEquals(projectInfoList.size(), pageSize);
            } else {
                // For the last page did we get right number of projects?
                Assert.assertEquals(projectInfoList.size(), activeCount % pageSize);
            }
            allActiveProjects.addAll(projectInfoList);  // Add to the list of active projects
            pageIndex++;
        } while (CollectionUtils.size(projectInfoList) == pageSize);

        PageRequest pageRequest = PageRequest.of(pageIndex, pageSize, sort);
        projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest, Boolean.FALSE);
        Assert.assertEquals(CollectionUtils.size(projectInfoList), 0);

        long allActiveProjectsCount = projectEntityMgr.countAllActiveProjects();

        Assert.assertEquals(CollectionUtils.size(allActiveProjects), allActiveProjectsCount);

    }

    private List<ProjectInfo> filterArchived (@NotNull List<ProjectInfo> projectInfoList) {
        return projectInfoList.stream().filter(pi -> BooleanUtils.isFalse(pi.getDeleted())).collect(Collectors.toList());
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
