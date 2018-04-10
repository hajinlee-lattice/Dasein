package com.latticeengines.apps.lp.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.lp.dao.BucketedScoreSummaryDao;
import com.latticeengines.apps.lp.entitymgr.BucketedScoreSummaryEntityMgr;
import com.latticeengines.apps.lp.repository.reader.BucketedScoreSummaryReaderRepository;
import com.latticeengines.apps.lp.repository.writer.BucketedScoreSummaryWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

@Component("bucketedScoreSummaryEntityMgr")
public class BucketedScoreSummaryEntityMgrImpl extends BaseEntityMgrRepositoryImpl<BucketedScoreSummary, Long>
        implements BucketedScoreSummaryEntityMgr {

    @Inject
    private BucketedScoreSummaryWriterRepository repository;

    @Inject
    private BucketedScoreSummaryReaderRepository readerRepository;

    @Inject
    private BucketedScoreSummaryDao dao;

    @Override
    public BaseJpaRepository<BucketedScoreSummary, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<BucketedScoreSummary> getDao() {
        return dao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public BucketedScoreSummary getByModelGuid(String modelGuid) {
        return repository.findByModelSummary_Id(modelGuid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public BucketedScoreSummary getByModelGuidFromReader(String modelGuid) {
        return readerRepository.findByModelSummary_Id(modelGuid);
    }

}
