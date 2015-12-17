package com.latticeengines.propdata.collection.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.propdata.collection.dao.PivotProgressDao;

@Component("pivotProgressDao")
public class PivotProgressDaoImpl extends ProgressDaoImplBase<PivotProgress>
        implements PivotProgressDao {

    @Override
    protected Class<PivotProgress> getEntityClass() {
        return PivotProgress.class;
    }

}
