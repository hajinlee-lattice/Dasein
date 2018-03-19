package com.latticeengines.pls.entitymanager.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.dao.BucketedScoreSummaryDao;
import com.latticeengines.pls.entitymanager.BucketedScoreSummaryEntityMgr;
import com.latticeengines.pls.repository.BucketedScoreSummaryRepository;

@Component("bucketedScoreSummaryEntityMgr")
public class BucketedScoreSummaryEntityMgrImpl extends BaseEntityMgrRepositoryImpl<BucketedScoreSummary, Long>
        implements BucketedScoreSummaryEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreSummary.class);

    @Inject
    private BucketedScoreSummaryRepository bucketedScoreSummaryRepository;

    @Inject
    private BucketedScoreSummaryDao bucketedScoreSummaryDao;

    @Override
    public BaseJpaRepository<BucketedScoreSummary, Long> getRepository() {
        return bucketedScoreSummaryRepository;
    }

    @Override
    public BaseDao<BucketedScoreSummary> getDao() {
        return bucketedScoreSummaryDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public BucketedScoreSummary findByModelSummary(ModelSummary modelSummary) {
        log.info("retrying to retrieve BucketedScoreSummary via model summary id {}", modelSummary.getPid());
        return bucketedScoreSummaryRepository.findByModelSummary(modelSummary);
    }

}
