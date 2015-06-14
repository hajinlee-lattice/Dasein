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
        Map<String, Object> result = wrapResult(recommendations);
        return result;
    }

    @Override
    public Map<String, Object> getPlays(String tenantName, int startId, int size) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PalymakerRecommendationDaoImpl dao = new PalymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> plays = dao.getPlays(startId, size);
        Map<String, Object> result = wrapResult(plays);
        return result;
    }

    @Override
    public Map<String, Object> getAccountextensions(String tenantName, int startId, int size) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PalymakerRecommendationDaoImpl dao = new PalymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> extensions = dao.getAccountExtensions(startId, size);
        Map<String, Object> result = wrapResult(extensions);
        return result;
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema(String tenantName) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PalymakerRecommendationDaoImpl dao = new PalymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> schemas = dao.getAccountExtensionSchema();
        return schemas;
    }

    @Override
    public Map<String, Object> getPlayValues(String tenantName, int startId, int size) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PalymakerRecommendationDaoImpl dao = new PalymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> values = dao.getPlayValues(startId, size);
        Map<String, Object> result = wrapResult(values);
        return result;
    }

    private Map<String, Object> wrapResult(List<Map<String, Object>> records) {
        Map<String, Object> result = new HashMap<>();
        if (records != null && records.size() > 0) {
            result.put(START_ID_KEY, records.get(0).get(ID_KEY));
            result.put(END_ID_KEY, records.get(records.size() - 1).get(ID_KEY));
            result.put(RECORDS_KEY, records);
        }
        return result;
    }

}
