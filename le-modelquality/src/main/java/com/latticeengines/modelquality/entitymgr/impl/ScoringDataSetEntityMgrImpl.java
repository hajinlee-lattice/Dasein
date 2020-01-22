package com.latticeengines.modelquality.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.ScoringDataSet;
import com.latticeengines.modelquality.dao.ScoringDataSetDao;
import com.latticeengines.modelquality.entitymgr.ScoringDataSetEntityMgr;

@Component("scoringDataSetEntityMgr")
public class ScoringDataSetEntityMgrImpl extends BaseEntityMgrImpl<ScoringDataSet> implements ScoringDataSetEntityMgr {

    @Inject
    private ScoringDataSetDao scoringDataSetDao;

    @Override
    public BaseDao<ScoringDataSet> getDao() {
        return scoringDataSetDao;
    }

}
