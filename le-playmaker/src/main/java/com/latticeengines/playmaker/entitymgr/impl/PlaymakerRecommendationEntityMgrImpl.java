package com.latticeengines.playmaker.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.playmaker.dao.impl.PalymakerRecommendationDaoImpl;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;

@Component("playmakerRecommendationEntityMgr")
public class PlaymakerRecommendationEntityMgrImpl implements PlaymakerRecommendationEntityMgr {

    @Autowired
    private JdbcTempalteFactoryImpl templateFactory;

    @Override
    public Map<String, Object> getRecommendations(String tenantName, int startId, int size) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PalymakerRecommendationDaoImpl dao = new PalymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> recommendations = dao.getRecommendations(startId, size);
        Map<String, Object> result = new HashMap<>();
        if (recommendations != null && recommendations.size() > 0) {
            result.put(START_ID_KEY, recommendations.get(0).get(ID_KEY));
            result.put(END_ID_KEY, recommendations.get(recommendations.size() - 1).get(ID_KEY));
            result.put(RECORDS_KEY, recommendations);
        }
        return result;
    }

    @Override
    public Map<String, Object> getPlays(String tenantName, int startId, int size) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PalymakerRecommendationDaoImpl dao = new PalymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> plays = dao.getPlays(startId, size);
        Map<String, Object> result = new HashMap<>();
        if (plays != null && plays.size() > 0) {
            result.put(START_ID_KEY, plays.get(0).get(ID_KEY));
            result.put(END_ID_KEY, plays.get(plays.size() - 1).get(ID_KEY));
            result.put(RECORDS_KEY, plays);
        }
        return result;
    }

}
