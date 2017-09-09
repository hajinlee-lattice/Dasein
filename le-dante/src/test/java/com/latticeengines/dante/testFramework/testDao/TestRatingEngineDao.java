package com.latticeengines.dante.testFramework.testDao;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.RatingEngine;

@Component("testRatingEngineDao")
public class TestRatingEngineDao extends BaseDaoImpl<RatingEngine> {

    @Override
    protected Class<RatingEngine> getEntityClass() {
        return RatingEngine.class;
    }
}
