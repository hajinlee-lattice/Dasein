package com.latticeengines.apps.dcp.entitymgr.impl;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.DCPProjectDao;
import com.latticeengines.apps.dcp.entitymgr.DCPProjectEntityMgr;
import com.latticeengines.apps.dcp.repository.DCPProjectRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.dcp.DCPProject;

@Component("dcpProjectEntityMgr")
public class DCPProjectEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<DCPProjectRepository, DCPProject, Long>
        implements DCPProjectEntityMgr {

    @Inject
    private DCPProjectEntityMgrImpl _self;

    @Inject
    private DCPProjectDao dcpProjectDao;

    @Resource(name = "DCPProjectReaderRepository")
    private DCPProjectRepository dcpProjectReaderRepository;

    @Resource(name = "DCPProjectWriterRepository")
    private DCPProjectRepository dcpProjectWriterRepository;

    @Override
    protected DCPProjectRepository getReaderRepo() {
        return dcpProjectReaderRepository;
    }

    @Override
    protected DCPProjectRepository getWriterRepo() {
        return dcpProjectWriterRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<DCPProjectRepository, DCPProject, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<DCPProject> getDao() {
        return dcpProjectDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DCPProject findByProjectId(String projectId) {
        if(isReaderConnection()) {
            return dcpProjectReaderRepository.findByProjectId(projectId);
        } else {
            return dcpProjectWriterRepository.findByProjectId(projectId);
        }
    }
}
