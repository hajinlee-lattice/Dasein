package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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

    private static final Boolean DONT_INCLUDE_ARCHIVED = Boolean.FALSE;
    private static final Boolean INCLUDE_ARCHIVED = Boolean.TRUE;

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
            projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest,
                    Boolean.FALSE /* false == include archived */);
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
        PageRequest page = PageRequest.of(0, 50, sort);
        List<String> projectIdList = new ArrayList<>();
        int totalCount = 20;
        // create some projects
        for (int i = 0; i < totalCount; i++) {
            projectIdList.add("Project_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        for (String s : projectIdList) {

            projectEntityMgr.create(generateProjectObject(s));
        }

        // Check that all 20 are created
        Assert.assertEquals(projectEntityMgr.findAllProjectInfo(page, Boolean.FALSE).size(), totalCount);

        // Now, Mark 4 projects as deleted
        List<Integer> rmIdx = Arrays.asList(2, 4, 7, 12); // delete the projects at these positions
        for (Integer idx : rmIdx) {
            Project project = projectEntityMgr.findByProjectId(projectIdList.get(idx));
            project.setDeleted(Boolean.TRUE);
            projectEntityMgr.update(project);

        }
        // activeCount is the count of not deleted projects (also know as not archived
        // projects)
        int activeCount = totalCount - rmIdx.size();

        Assert.assertEquals((long)projectEntityMgr.countAllProjects(), totalCount);
        Assert.assertEquals((long)projectEntityMgr.countAllActiveProjects(), activeCount);

        // check that finding all projects including archived returns the total count
        Assert.assertEquals(projectEntityMgr.findAllProjectInfo(page, Boolean.TRUE).size(), totalCount);

        // check that finding all projects not including archived returns the active
        // count.
        Assert.assertEquals(filterArchived(projectEntityMgr.findAllProjectInfo(page, Boolean.FALSE)).size(),
                activeCount);

        List<ProjectInfo> allActiveProjects = new ArrayList<>();
        List<ProjectInfo> projectInfoList;
        int pageSize = 3;
        int pageIndex = 0;
        do {
            PageRequest pageRequest = PageRequest.of(pageIndex, pageSize, sort);
            projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest,
                    Boolean.FALSE /* false == dont include archived */);
            if (pageIndex < Math.floor((float) activeCount / pageSize)) {
                // Did we get 1 page sized amount of projects?
                Assert.assertEquals(projectInfoList.size(), pageSize);
            } else {
                // For the last page did we get right number of projects?
                Assert.assertEquals(projectInfoList.size(), activeCount % pageSize);
            }
            allActiveProjects.addAll(projectInfoList); // Add to the list of active projects
            pageIndex++;
        } while (CollectionUtils.size(projectInfoList) == pageSize);

        PageRequest pageRequest = PageRequest.of(pageIndex, pageSize, sort);
        projectInfoList = projectEntityMgr.findAllProjectInfo(pageRequest, Boolean.FALSE);
        Assert.assertEquals(CollectionUtils.size(projectInfoList), 0);

        long allActiveProjectsCount = projectEntityMgr.countAllActiveProjects();

        Assert.assertEquals(CollectionUtils.size(allActiveProjects), allActiveProjectsCount);

    }

    private List<ProjectInfo> filterArchived(@NotNull List<ProjectInfo> projectInfoList) {
        return projectInfoList.stream().filter(pi -> BooleanUtils.isFalse(pi.getDeleted()))
                .collect(Collectors.toList());
    }

    @Test(groups = "functional")
    public void testFields() {
        String projectId = "Project_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
        String description = getRandomDescription();
        projectEntityMgr.create(generateProjectObject(projectId, description));

        ProjectInfo projectInfo = projectEntityMgr.findProjectInfoByProjectId(projectId);
        Assert.assertEquals(description, projectInfo.getProjectDescription());
        Assert.assertEquals(projectId, projectInfo.getProjectDisplayName());
        Assert.assertEquals(String.format("Projects/%s/", projectId), projectInfo.getRootPath());
        Assert.assertFalse(projectInfo.getDeleted());

        // Really delete project so it doesn't get counted in other tests
        cleanUpProjects(projectId);
    }

    private List<String> createProjects (int numToCreate) {
        List<String> projectIdList = new ArrayList<>(numToCreate);
        for (int j=0; j<numToCreate; j++) {
            projectIdList.add("Project_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }

        String teamId = "Team_" + RandomStringUtils.randomAlphanumeric(7).toLowerCase();
        String description = getRandomDescription();
        for (String projectId : projectIdList) {
            projectEntityMgr.create(generateProjectObject(projectId, description, teamId));

        }
        return projectIdList;
    }

    @Test(groups = "functional")
    public void testInTeamIdWithArchived() {

        List<String> projectIdList = createProjects(6);

        // delete some projects
        Project project1 = projectEntityMgr.findByProjectId(projectIdList.get(1));
        project1.setDeleted(Boolean.TRUE);
        projectEntityMgr.update(project1);

        Project project3 = projectEntityMgr.findByProjectId(projectIdList.get(3));
        project3.setDeleted(Boolean.TRUE);
        projectEntityMgr.update(project3);

        Project project0 = projectEntityMgr.findByProjectId(projectIdList.get(0));
        String teamId = project0.getTeamId();

        List<String> teamIdList = Arrays.asList("TeamOne", "TeamTwo", "TeamThree", teamId);

        List<ProjectInfo> projectInfoList = projectEntityMgr.findAllProjectInfoInTeamIds(Pageable.unpaged(), teamIdList, DONT_INCLUDE_ARCHIVED);
        Assert.assertEquals(projectInfoList.size(), 4);

        List<ProjectInfo> projectInfoWithArchivedList = projectEntityMgr.findAllProjectInfoInTeamIds(Pageable.unpaged(), teamIdList, INCLUDE_ARCHIVED);
        Assert.assertEquals(projectInfoWithArchivedList.size(), 6);

        cleanUpProjects(projectIdList);
    }

    @Test(groups = "functional")
    public void testInTeamId() {
        Sort sort = Sort.by(Sort.Direction.DESC, "updated");
        PageRequest pageRequest = PageRequest.of(0, 20, sort);

        String teamId = "Team_" + RandomStringUtils.randomAlphanumeric(7).toLowerCase();
        String projectId = "Project_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
        String description = getRandomDescription();
        projectEntityMgr.create(generateProjectObject(projectId, description, teamId));

        List<String> teamIdList = Arrays.asList("notTheTeamId", "alsoNotTheTeamId", "farFromTheTeamId");
        List<ProjectInfo> emptyList = projectEntityMgr.findAllProjectInfoInTeamIds(pageRequest, teamIdList, DONT_INCLUDE_ARCHIVED);
        Assert.assertTrue(emptyList.isEmpty());

        List<String> updatedTeamIdList = new ArrayList<>(teamIdList);
        updatedTeamIdList.add(teamId);
        List<ProjectInfo> containsOneList = projectEntityMgr.findAllProjectInfoInTeamIds(pageRequest,
                updatedTeamIdList, DONT_INCLUDE_ARCHIVED);
        Assert.assertEquals(1, containsOneList.size());

        String projectId2 = "Project_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
        String desc2 = getRandomDescription();
        projectEntityMgr.create(generateProjectObject(projectId2, desc2, teamId));

        List<ProjectInfo> containsTwoList = projectEntityMgr.findAllProjectInfoInTeamIds(pageRequest,
                updatedTeamIdList, DONT_INCLUDE_ARCHIVED);
        Assert.assertEquals(2, containsTwoList.size());

        // Check that the two projects are different
        Assert.assertNotEquals(containsTwoList.get(0).getProjectDisplayName(),
                containsTwoList.get(1).getProjectDisplayName());

        // Now check that findProjectInfoByProjectIdInTeamIds properly filters projects
        // in other teams
        ProjectInfo noProjectInfo = projectEntityMgr.findProjectInfoByProjectIdInTeamIds(projectId, teamIdList);
        Assert.assertNull(noProjectInfo); // shouldn't find because not in team list

        ProjectInfo foundProjectInfo = projectEntityMgr.findProjectInfoByProjectIdInTeamIds(projectId,
                updatedTeamIdList); // It is in the updated team list
        Assert.assertNotNull(foundProjectInfo);
        Assert.assertEquals(projectId, foundProjectInfo.getProjectId());

        cleanUpProjects(projectId, projectId2);
    }

    private void cleanUpProjects(String... projectIds) {
        for (String projectId : projectIds) {
            Project project = projectEntityMgr.findByProjectId(projectId);
            projectEntityMgr.delete(project);
        }
    }

    private void cleanUpProjects(List<String> projectIdList) {
        for (String projectId : projectIdList) {
            Project project = projectEntityMgr.findByProjectId(projectId);
            projectEntityMgr.delete(project);
        }
    }

    private String getRandomDescription() {
        List<String> descriptionList = Arrays.asList("A test project 1", "A test project 2");
        return descriptionList.get(RandomUtils.nextInt(0, descriptionList.size()));
    }

    private Project generateProjectObject(String projectId) {
        return generateProjectObject(projectId, getRandomDescription());
    }

    private Project generateProjectObject(String projectId, String description) {
        return generateProjectObject(projectId, description, null);
    }

    private Project generateProjectObject(String projectId, String description, String teamId) {
        Project project = new Project();
        project.setCreatedBy("testdcp@dnb.com");
        project.setProjectDisplayName(projectId);
        project.setProjectId(projectId);
        project.setTenant(MultiTenantContext.getTenant());
        project.setDeleted(Boolean.FALSE);
        project.setProjectType(Project.ProjectType.Type1);
        project.setRootPath(String.format("Projects/%s/", projectId));
        project.setProjectDescription(description);
        project.setTeamId(teamId);
        return project;
    }
}
