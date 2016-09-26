package com.latticeengines.modelquality.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.modelquality.dao.AnalyticTestDao;
import com.latticeengines.modelquality.entitymgr.AnalyticTestEntityMgr;

@Component("qualityAnalyticTestEntityMgr")
public class AnalyticTestEntityMgrImpl extends BaseEntityMgrImpl<AnalyticTest> implements AnalyticTestEntityMgr {

    @Autowired
    private AnalyticTestDao analyticTestDao;

    @Override
    public BaseDao<AnalyticTest> getDao() {
        return analyticTestDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(AnalyticTest analyticTest) {
        analyticTestDao.create(analyticTest);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AnalyticTest findByName(String analyticTestName) {
        return analyticTestDao.findByField("NAME", analyticTestName);
    }

}
