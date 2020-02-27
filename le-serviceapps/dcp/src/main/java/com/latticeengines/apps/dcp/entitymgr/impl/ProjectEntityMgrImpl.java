package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.ProjectDao;
import com.latticeengines.apps.dcp.entitymgr.ProjectEntityMgr;
import com.latticeengines.apps.dcp.repository.ProjectRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;

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
        if(isReaderConnection()) {
            return projectReaderRepository.findByProjectId(projectId);
        } else {
            return projectWriterRepository.findByProjectId(projectId);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Project findByImportSystem(S3ImportSystem importSystem) {
        if (isReaderConnection()) {
            return projectReaderRepository.findByImportSystem(importSystem);
        } else {
            return projectWriterRepository.findByImportSystem(importSystem);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Project> findAll() {
        if(isReaderConnection()) {
            return projectReaderRepository.findAll();
        } else {
            return projectWriterRepository.findAll();
        }
    }
}
