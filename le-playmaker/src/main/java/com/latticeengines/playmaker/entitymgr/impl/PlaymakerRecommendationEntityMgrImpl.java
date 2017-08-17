package com.latticeengines.playmaker.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;
import com.latticeengines.playmaker.entitymgr.PlaymakerDaoFactory;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;

@Component("playmakerRecommendationEntityMgr")
public class PlaymakerRecommendationEntityMgrImpl implements PlaymakerRecommendationEntityMgr {

    @Autowired
    private PlaymakerDaoFactory daoFactory;

    @Override
    public Map<String, Object> getRecommendations(String tenantName, String lookupSource, long start, int offset,
            int maximum, int syncDestination, List<String> playIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> recommendations = dao.getRecommendations(start, offset, maximum, syncDestination,
                playIds);
        Map<String, Object> result = wrapResult(recommendations);
        return result;
    }

    @Override
    public Map<String, Object> getRecommendationCount(String tenantName, String lookupSource, long start,
            int syncDestination, List<String> playIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);
        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getRecommendationCount(start, syncDestination, playIds));
        return result;
    }

    @Override
    public Map<String, Object> getPlays(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<Integer> playgroupIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> plays = dao.getPlays(start, offset, maximum, playgroupIds);
        Map<String, Object> result = wrapResult(plays);
        return result;
    }

    @Override
    public Map<String, Object> getPlayCount(String tenantName, String lookupSource, long start,
            List<Integer> playgroupIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getPlayCount(start, playgroupIds));
        return result;
    }

    @Override
    public Map<String, Object> getAccountExtensions(String tenantName, String lookupSource, long start, int offset,
            int maximum, List<String> accountIds, String filterBy, Long recStart, String columns,
            boolean hasSfdcContactId) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> extensions = dao.getAccountExtensions(start, offset, maximum, accountIds, filterBy,
                recStart, columns, hasSfdcContactId);
        Map<String, Object> result = wrapResult(extensions);
        return result;
    }

    @Override
    public Map<String, Object> getAccountextExsionCount(String tenantName, String lookupSource, long start,
            List<String> accountIds, String filterBy, Long recStart) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getAccountExtensionCount(start, accountIds, filterBy, recStart));
        return result;
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> schemas = dao.getAccountExtensionSchema();
        return schemas;
    }

    @Override
    public Map<String, Object> getAccountExtensionColumnCount(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getAccountExtensionColumnCount());
        return result;
    }

    @Override
    public Map<String, Object> getContacts(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<Integer> contactIds, List<Integer> accountIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> contacts = dao.getContacts(start, offset, maximum, contactIds, accountIds);
        Map<String, Object> result = wrapResult(contacts);
        return result;
    }

    @Override
    public Map<String, Object> getContactCount(String tenantName, String lookupSource, long start,
            List<Integer> contactIds, List<Integer> accountIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getContactCount(start, contactIds, accountIds));
        return result;
    }

    @Override
    public Map<String, Object> getContactExtensions(String tenantName, String lookupSource, long start, int offset,
            int maximum, List<Integer> accountIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> extensions = dao.getContactExtensions(start, offset, maximum, accountIds);
        Map<String, Object> result = wrapResult(extensions);
        return result;
    }

    @Override
    public Map<String, Object> getContactExtensionCount(String tenantName, String lookupSource, long start,
            List<Integer> accountIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getContactExtensionCount(start, accountIds));
        return result;
    }

    @Override
    public List<Map<String, Object>> getContactExtensionSchema(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);
        List<Map<String, Object>> schemas = dao.getContactExtensionSchema();
        return schemas;
    }

    @Override
    public Map<String, Object> getContactExtensionColumnCount(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getContactExtensionColumnCount());
        return result;
    }

    @Override
    public Map<String, Object> getPlayValues(String tenantName, String lookupSource, long start, int offset,
            int maximum, List<Integer> playgroupIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> values = dao.getPlayValues(start, offset, maximum, playgroupIds);
        Map<String, Object> result = wrapResult(values);
        return result;
    }

    @Override
    public Map<String, Object> getPlayValueCount(String tenantName, String lookupSource, long start,
            List<Integer> playgroupIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getPlayValueCount(start, playgroupIds));
        return result;
    }

    @Override
    public List<Map<String, Object>> getWorkflowTypes(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);
        return dao.getWorkflowTypes();
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

    @Override
    public List<Map<String, Object>> getPlayGroups(String tenantName, String lookupSource, long start, int offset,
            int maximum) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);
        return dao.getPlayGroups(start, offset, maximum);
    }

    @Override
    public Map<String, Object> getPlayGroupCount(String tenantName, String lookupSource, long start) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getPlayGroupCount(start));
        return result;

    }

}
