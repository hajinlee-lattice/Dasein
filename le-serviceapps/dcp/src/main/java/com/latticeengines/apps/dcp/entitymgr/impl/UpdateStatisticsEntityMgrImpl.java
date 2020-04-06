package com.latticeengines.apps.dcp.entitymgr.impl;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.UploadStatisticsDao;
import com.latticeengines.apps.dcp.entitymgr.UpdateStatisticsEntityMgr;
import com.latticeengines.apps.dcp.repository.UploadStatisticsRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadStatsContainer;


@Component("updateStatisticsEntityMgr")
public class UpdateStatisticsEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<UploadStatisticsRepository, UploadStatsContainer, Long>
        implements UpdateStatisticsEntityMgr {

    @Inject
    private UpdateStatisticsEntityMgrImpl _self;

    @Inject
    private UploadStatisticsDao statisticsDao;

    @Resource(name = "uploadStatisticsReaderRepository")
    private UploadStatisticsRepository readerRepository;

    @Resource(name = "uploadStatisticsWriterRepository")
    private UploadStatisticsRepository writerRepository;

    @Override
    protected UploadStatisticsRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected UploadStatisticsRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<UploadStatisticsRepository, UploadStatsContainer, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<UploadStatsContainer> getDao() {
        return statisticsDao;
    }


    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public UploadStatsContainer save(UploadStatsContainer container) {
        container.setIsLatest(false);
        getDao().createOrUpdate(container);
        return container;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public UploadStatsContainer findByUploadAndId(Upload upload, Long statsId) {
        return getReadOrWriteRepository().findByUploadAndPid(upload, statsId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public UploadStatsContainer findIsLatest(Upload upload) {
        return getReadOrWriteRepository().findByUploadAndIsLatest(upload, true);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public UploadStatsContainer setAsLatest(UploadStatsContainer container) {
        container.setIsLatest(true);
        getDao().createOrUpdate(container);
        return container;
    }

}
