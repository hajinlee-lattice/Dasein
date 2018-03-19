package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.pls.dao.BucketedScoreSummaryDao;

@Component("bucketedScoreSummaryDao")
public class BucketedScoreSummaryDaoImpl extends BaseDaoImpl<BucketedScoreSummary> implements BucketedScoreSummaryDao {

    @Override
    protected Class<BucketedScoreSummary> getEntityClass() {
        return BucketedScoreSummary.class;
    }

}
