package com.latticeengines.playmaker.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImpl;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;

@Component("playmakerRecommendationEntityMgr")
public class PlaymakerRecommendationEntityMgrImpl implements PlaymakerRecommendationEntityMgr {

    @Autowired
    private JdbcTemplateFactoryImpl templateFactory;

    @Override
    public Map<String, Object> getRecommendations(String tenantName, long start, int offset, int maximum) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> recommendations = dao.getRecommendations(start, offset, maximum);
        Map<String, Object> result = wrapResult(recommendations);
        return result;
    }

    @Override
    public Map<String, Object> getRecommendationCount(String tenantName, long start) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);
        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getRecommendationCount(start));
        return result;
    }

    @Override
    public Map<String, Object> getPlays(String tenantName, long start, int offset, int maximum) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> plays = dao.getPlays(start, offset, maximum);
        Map<String, Object> result = wrapResult(plays);
        return result;
    }

    @Override
    public Map<String, Object> getPlayCount(String tenantName, long start) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getPlayCount(start));
        return result;
    }

    @Override
    public Map<String, Object> getAccountextensions(String tenantName, long start, int offset, int maximum) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> extensions = dao.getAccountExtensions(start, offset, maximum);
        Map<String, Object> result = wrapResult(extensions);
        return result;
    }

    @Override
    public Map<String, Object> getAccountextensionCount(String tenantName, long start) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getAccountExtensionCount(start));
        return result;
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema(String tenantName) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> schemas = dao.getAccountExtensionSchema();
        return schemas;
    }

    @Override
    public Map<String, Object> getPlayValues(String tenantName, long start, int offset, int maximum) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        List<Map<String, Object>> values = dao.getPlayValues(start, offset, maximum);
        Map<String, Object> result = wrapResult(values);
        return result;
    }

    @Override
    public Map<String, Object> getPlayValueCount(String tenantName, long start) {
        NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
        PlaymakerRecommendationDaoImpl dao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getPlayValueCount(start));
        return result;
    }

    private Map<String, Object> wrapResult(List<Map<String, Object>> records) {
        Map<String, Object> result = new HashMap<>();
        if (records != null && records.size() > 0) {
            result.put(START_KEY, records.get(0).get(LAST_MODIFIATION_DATE_KEY));
            result.put(END_KEY, records.get(records.size() - 1).get(LAST_MODIFIATION_DATE_KEY));
            result.put(RECORDS_KEY, records);
        }
        return result;
    }

}
