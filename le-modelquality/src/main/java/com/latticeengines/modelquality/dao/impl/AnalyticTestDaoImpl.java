package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.modelquality.dao.AnalyticTestDao;

@Component("qualityAnalyticTestDao")
public class AnalyticTestDaoImpl extends BaseDaoImpl<AnalyticTest> implements AnalyticTestDao {

    @Override
    protected Class<AnalyticTest> getEntityClass() {
        return AnalyticTest.class;
    }

}
