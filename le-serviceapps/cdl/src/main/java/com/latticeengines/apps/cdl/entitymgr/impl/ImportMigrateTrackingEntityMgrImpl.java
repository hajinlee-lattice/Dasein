package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.ImportMigrateTrackingDao;
import com.latticeengines.apps.cdl.entitymgr.ImportMigrateTrackingEntityMgr;
import com.latticeengines.apps.cdl.repository.ImportMigrateTrackingRepository;
import com.latticeengines.apps.cdl.repository.reader.ImportMigrateTrackingReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.ImportMigrateTrackingWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;

@Component("importMigrateTrackingEntityMgr")
public class ImportMigrateTrackingEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<ImportMigrateTrackingRepository, ImportMigrateTracking, Long>
        implements ImportMigrateTrackingEntityMgr {

    @Inject
    private ImportMigrateTrackingEntityMgrImpl _self;

    @Inject
    private ImportMigrateTrackingDao importMigrateTrackingDao;

    @Inject
    private ImportMigrateTrackingReaderRepository readerRepository;

    @Inject
    private ImportMigrateTrackingWriterRepository writerRepository;

    @Override
    protected ImportMigrateTrackingRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected ImportMigrateTrackingRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<ImportMigrateTrackingRepository, ImportMigrateTracking, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<ImportMigrateTracking> getDao() {
        return importMigrateTrackingDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ImportMigrateTracking findByPid(Long pid) {
        if (isReaderConnection()) {
            return readerRepository.findByPid(pid);
        } else {
            return writerRepository.findByPid(pid);
        }
    }
}
