package com.latticeengines.apps.lp.entitymgr.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.lp.dao.BucketedScoreSummaryDao;
import com.latticeengines.apps.lp.entitymgr.BucketedScoreSummaryEntityMgr;
import com.latticeengines.apps.lp.repository.writer.BucketedScoreSummaryWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

@Component("bucketedScoreSummaryEntityMgr")
public class BucketedScoreSummaryEntityMgrImpl extends BaseEntityMgrRepositoryImpl<BucketedScoreSummary, Long>
        implements BucketedScoreSummaryEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreSummary.class);

    @Inject
    private BucketedScoreSummaryWriterRepository repository;

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
    public BucketedScoreSummary findByModelGuid(String modelGuid) {
        log.info("retrying to retrieve BucketedScoreSummary via model GUID {}", modelGuid);
        return repository.findByModelSummary_Id(modelGuid);
    }

}
