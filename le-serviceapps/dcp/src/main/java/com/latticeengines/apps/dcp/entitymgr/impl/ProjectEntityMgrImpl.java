package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.ProjectDao;
import com.latticeengines.apps.dcp.entitymgr.ProjectEntityMgr;
import com.latticeengines.apps.dcp.repository.ProjectRepository;
import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("projectEntityMgr")
public class ProjectEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<ProjectRepository, Project, Long>
        implements ProjectEntityMgr {

    @Inject
    private ProjectEntityMgrImpl _self;

    @Inject
    private ProjectDao projectDao;

    @Resource(name = "projectReaderRepository")
    private ProjectRepository projectReaderRepository;

    @Resource(name = "projectWriterRepository")
    private ProjectRepository projectWriterRepository;

    @Override
    protected ProjectRepository getReaderRepo() {
        return projectReaderRepository;
    }

    @Override
    protected ProjectRepository getWriterRepo() {
        return projectWriterRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<ProjectRepository, Project, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<Project> getDao() {
        return projectDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Project findByProjectId(String projectId) {
        Project project = getReadOrWriteRepository().findByProjectId(projectId);
        inflateSystem(project);
        return project;
    }

//    @Override
//    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
//    public Project findByImportSystem(S3ImportSystem importSystem) {
//        Project project = getReadOrWriteRepository().findByImportSystem(importSystem);
//        inflateSystem(project);
//        return project;
//    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ProjectInfo findProjectInfoByProjectId(String projectId) {
        List<Object[]> result = getReadOrWriteRepository().findProjectInfoByProjectId(projectId);
        if (CollectionUtils.isEmpty(result)) {
            return null;
        } else {
            return getProjectInfo(result.get(0));
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ProjectInfo> findAllProjectInfo() {
        List<Object[]> result = getReadOrWriteRepository().findAllProjects();
        if (CollectionUtils.isEmpty(result)) {
            return Collections.emptyList();
        } else {
            return result.stream().map(this::getProjectInfo).collect(Collectors.toList());
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ProjectInfo findProjectInfoBySourceId(String sourceId) {
        List<Object[]> result = getReadOrWriteRepository().findProjectInfoBySourceId(sourceId);
        if (CollectionUtils.isEmpty(result)) {
            return null;
        } else {
            return getProjectInfo(result.get(0));
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public S3ImportSystem findImportSystemByProjectId(String projectId) {
        return getReadOrWriteRepository().findImportSystemByProjectId(projectId);
    }

    @SuppressWarnings("unchecked")
    private ProjectInfo getProjectInfo(Object[] columns) {
        ProjectInfo info = new ProjectInfo();
        info.setProjectId((String) columns[0]);
        info.setProjectDisplayName((String) columns[1]);
        info.setRootPath((String) columns[2]);
        info.setDeleted((Boolean) columns[3]);
        info.setCreated((Date) columns[4]);
        info.setUpdated((Date) columns[5]);
        info.setCreatedBy((String) columns[6]);
        info.setRecipientList((List<String>) columns[7]);
        info.setSystemId((Long) columns[8]);
        return info;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Project> findAll() {
        List<Project> projectList = getReadOrWriteRepository().findAll();
        if (CollectionUtils.isNotEmpty(projectList)) {
            projectList.forEach(this::inflateSystem);
        }
        return projectList;
    }

    private void inflateSystem(Project project) {
        if (project == null) {
            return;
        }
        HibernateUtils.inflateDetails(project.getS3ImportSystem());
        HibernateUtils.inflateDetails(project.getS3ImportSystem().getTasks());
        for (DataFeedTask datafeedTask : project.getS3ImportSystem().getTasks()) {
            TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(datafeedTask.getImportData());
        }
    }
}
