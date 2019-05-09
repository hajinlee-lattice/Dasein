package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.AtlasSchedulingDao;
import com.latticeengines.apps.cdl.entitymgr.AtlasSchedulingEntityMgr;
import com.latticeengines.apps.cdl.repository.AtlasSchedulingRepository;
import com.latticeengines.apps.cdl.repository.reader.AtlasSchedulingReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.AtlasSchedulingWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;

@Component("atlasSchedulingEntityMgr")
public class AtlasSchedulingEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<AtlasSchedulingRepository, AtlasScheduling, Long>
        implements AtlasSchedulingEntityMgr {

    @Inject
    private AtlasSchedulingEntityMgrImpl _self;

    @Inject
    private AtlasSchedulingDao atlasSchedulingDao;

    @Inject
    private AtlasSchedulingReaderRepository readerRepository;

    @Inject
    private AtlasSchedulingWriterRepository writerRepository;

    @Override
    protected AtlasSchedulingRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected AtlasSchedulingRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<AtlasSchedulingRepository, AtlasScheduling, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<AtlasScheduling> getDao() {
        return atlasSchedulingDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void createSchedulingObj(AtlasScheduling atlasScheduling) {
        atlasSchedulingDao.create(atlasScheduling);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateSchedulingObj(AtlasScheduling atlasScheduling) {
        atlasSchedulingDao.update(atlasScheduling);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AtlasScheduling findAtlasSchedulingByType(AtlasScheduling.ScheduleType type) {
        if (isReaderConnection()) {
            return readerRepository.findByType(type);
        } else {
            return writerRepository.findByType(type);
        }
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AtlasScheduling> getAllAtlasSchedulingByType(AtlasScheduling.ScheduleType type) {
        if (isReaderConnection()) {
            return readerRepository.findAllByType(type);
        } else {
            return writerRepository.findAllByType(type);
        }
    }

}
