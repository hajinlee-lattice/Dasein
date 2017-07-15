package com.latticeengines.playmaker.dao.impl;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.playmaker.dao.LpiPMRecommendationDao;

@Component("lpiPMRecommendationDao")
public class LpiPMRecommendationDaoImpl extends BaseDaoImpl<Recommendation> implements LpiPMRecommendationDao {

    @Override
    protected Class<Recommendation> getEntityClass() {
        return Recommendation.class;
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maOximum, int syncDestination,
            List<String> playIds) {
        return null;
    }

    @Override
    public int getRecommendationCount(long start, int syncDestination, List<String> playIds) {
        return 0;
    }

}
