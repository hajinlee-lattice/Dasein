package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.PlayTypeDao;
import com.latticeengines.apps.cdl.entitymgr.PlayTypeEntityMgr;
import com.latticeengines.apps.cdl.repository.PlayTypeRepository;
import com.latticeengines.apps.cdl.repository.reader.PlayTypeReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.PlayTypeWriterRepository;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.PlayType;

@Component("playTypeEntityMgr")
public class PlayTypeEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<PlayTypeRepository, PlayType, Long>
        implements PlayTypeEntityMgr {

    @Inject
    private PlayTypeDao dao;

    @Inject
    private PlayTypeEntityMgrImpl _self;

    @Inject
    private PlayTypeWriterRepository repository;

    @Inject
    private PlayTypeReaderRepository readerRepository;

    @Override
    public BaseJpaRepository<PlayType, Long> getRepository() {
        return repository;
    }

    @Override
    public PlayTypeDao getDao() {
        return dao;
    }

    @Override
    protected PlayTypeRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected PlayTypeRepository getWriterRepo() {
        return repository;
    }

    @Override
    protected PlayTypeEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayType findById(String id) { return readerRepository.findById(id); }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayType findByPid(Long pid) { return readerRepository.findByPid(pid); }
}
