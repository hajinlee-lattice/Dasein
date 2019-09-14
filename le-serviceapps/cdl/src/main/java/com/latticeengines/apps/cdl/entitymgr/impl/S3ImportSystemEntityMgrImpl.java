package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.S3ImportSystemDao;
import com.latticeengines.apps.cdl.entitymgr.S3ImportSystemEntityMgr;
import com.latticeengines.apps.cdl.repository.S3ImportSystemRepository;
import com.latticeengines.apps.cdl.repository.reader.S3ImportSystemReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.S3ImportSystemWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

@Component("s3ImportSystemEntityMgr")
public class S3ImportSystemEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<S3ImportSystemRepository, S3ImportSystem, Long>
        implements S3ImportSystemEntityMgr {

    @Inject
    private S3ImportSystemEntityMgrImpl _self;

    @Inject
    private S3ImportSystemDao s3ImportSystemDao;

    @Inject
    private S3ImportSystemReaderRepository readerRepository;

    @Inject
    private S3ImportSystemWriterRepository writerRepository;

    @Override
    protected S3ImportSystemRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected S3ImportSystemRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<S3ImportSystemRepository, S3ImportSystem, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<S3ImportSystem> getDao() {
        return s3ImportSystemDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void createS3ImportSystem(S3ImportSystem importSystem) {
        s3ImportSystemDao.create(importSystem);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public S3ImportSystem findS3ImportSystem(String name) {
        if (isReaderConnection()) {
            return readerRepository.findByName(name);
        } else {
            return writerRepository.findByName(name);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<S3ImportSystem> findByMapToLatticeAccount(Boolean mapToLatticeAccount) {
        if (isReaderConnection()) {
            return readerRepository.findByMapToLatticeAccount(mapToLatticeAccount);
        } else {
            return writerRepository.findByMapToLatticeAccount(mapToLatticeAccount);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<S3ImportSystem> findByMapToLatticeContact(Boolean mapToLatticeContact) {
        if (isReaderConnection()) {
            return readerRepository.findByMapToLatticeContact(mapToLatticeContact);
        } else {
            return writerRepository.findByMapToLatticeContact(mapToLatticeContact);
        }
    }
}
