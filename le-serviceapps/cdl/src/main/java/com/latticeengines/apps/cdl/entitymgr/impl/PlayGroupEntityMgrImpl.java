package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;
import javax.inject.Inject;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.latticeengines.apps.cdl.dao.PlayGroupDao;
import com.latticeengines.apps.cdl.entitymgr.PlayGroupEntityMgr;
import com.latticeengines.apps.cdl.repository.PlayGroupRepository;
import com.latticeengines.apps.cdl.repository.reader.PlayGroupReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.PlayGroupWriterRepository;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.PlayGroup;

@Component("playGroupEntityMgr")
public class PlayGroupEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<PlayGroupRepository, PlayGroup, Long>
        implements PlayGroupEntityMgr {

    @Inject
    private PlayGroupDao dao;

    @Inject
    private PlayGroupEntityMgrImpl _self;

    @Inject
    private PlayGroupWriterRepository repository;

    @Inject
    private PlayGroupReaderRepository readerRepository;

    @Override
    public BaseJpaRepository<PlayGroup, Long> getRepository() {
        return repository;
    }

    @Override
    public PlayGroupDao getDao() {
        return dao;
    }

    @Override
    protected PlayGroupRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected PlayGroupRepository getWriterRepo() {
        return repository;
    }

    @Override
    protected PlayGroupEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayGroup> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayGroup findById(String id) {
        return readerRepository.findById(id);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayGroup findByPid(Long pid) {
        return readerRepository.findByPid(pid);
    }
}
