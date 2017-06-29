package com.latticeengines.datadb.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datadb.dao.RecommendationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datadb.Recommendation;

@Component("recommendationDao")
public class RecommendationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Recommendation>
        implements RecommendationDao {

    @Override
    protected Class<Recommendation> getEntityClass() {
        return Recommendation.class;
    }

    @Override
    public Recommendation findByLaunchId(String launchId) {
        return null;
    }
}
