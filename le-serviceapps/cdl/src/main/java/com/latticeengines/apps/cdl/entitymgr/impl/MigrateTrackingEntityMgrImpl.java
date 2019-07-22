package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.MigrateTrackingDao;
import com.latticeengines.apps.cdl.entitymgr.MigrateTrackingEntityMgr;
import com.latticeengines.apps.cdl.repository.MigrateTrackingRepository;
import com.latticeengines.apps.cdl.repository.reader.MigrateTrackingReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.MigrateTrackingWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;

@Component("migrateTrackingEntityMgr")
public class MigrateTrackingEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<MigrateTrackingRepository, MigrateTracking, Long>
        implements MigrateTrackingEntityMgr {

    @Inject
    private MigrateTrackingEntityMgrImpl _self;

    @Inject
    private MigrateTrackingDao migrateTrackingDao;

    @Inject
    private MigrateTrackingReaderRepository readerRepository;

    @Inject
    private MigrateTrackingWriterRepository writerRepository;

    @Override
    protected MigrateTrackingRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected MigrateTrackingRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<MigrateTrackingRepository, MigrateTracking, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<MigrateTracking> getDao() {
        return migrateTrackingDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MigrateTracking findByPid(Long pid) {
        if (isReaderConnection()) {
            return readerRepository.findByPid(pid);
        } else {
            return writerRepository.findByPid(pid);
        }
    }
}
