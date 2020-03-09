package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.List;

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
        Project project;
        if(isReaderConnection()) {
            project = projectReaderRepository.findByProjectId(projectId);
        } else {
            project = projectWriterRepository.findByProjectId(projectId);
        }
        inflateSystem(project);
        return project;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Project findByImportSystem(S3ImportSystem importSystem) {
        Project project;
        if (isReaderConnection()) {
            project = projectReaderRepository.findByImportSystem(importSystem);
        } else {
            project = projectWriterRepository.findByImportSystem(importSystem);
        }
        inflateSystem(project);
        return project;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Project> findAll() {
        List<Project> projectList;
        if(isReaderConnection()) {
            projectList = projectReaderRepository.findByDeletedNot(Boolean.TRUE);
        } else {
            projectList = projectWriterRepository.findByDeletedNot(Boolean.TRUE);
        }
        if (CollectionUtils.isNotEmpty(projectList)) {
            projectList.forEach(this::inflateSystem);
        }
        return projectList;
    }

    private void inflateSystem(Project project) {
        if (project == null) {
            return;
        }
        HibernateUtils.inflateDetails(project.getS3ImportSystem().getTasks());
        for (DataFeedTask datafeedTask : project.getS3ImportSystem().getTasks()) {
            TableEntityMgr.inflateTable(datafeedTask.getImportTemplate());
            TableEntityMgr.inflateTable(datafeedTask.getImportData());
        }
    }
}
