package com.latticeengines.apps.lp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.dao.BucketedScoreSummaryDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

@Component("bucketedScoreSummaryDao")
public class BucketedScoreSummaryDaoImpl extends BaseDaoImpl<BucketedScoreSummary> implements BucketedScoreSummaryDao {

    @Override
    protected Class<BucketedScoreSummary> getEntityClass() {
        return BucketedScoreSummary.class;
    }

}
