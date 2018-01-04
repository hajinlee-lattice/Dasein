package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.ScoringDataSet;
import com.latticeengines.modelquality.dao.ScoringDataSetDao;

@Component("scoringDataSetDao")
public class ScoringDataSetDaoImpl extends ModelQualityBaseDaoImpl<ScoringDataSet> implements ScoringDataSetDao {

    @Override
    protected Class<ScoringDataSet> getEntityClass() {
        return ScoringDataSet.class;
    }

}
