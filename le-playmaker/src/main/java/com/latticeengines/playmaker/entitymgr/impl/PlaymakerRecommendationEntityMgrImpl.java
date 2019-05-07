package com.latticeengines.playmaker.entitymgr.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;
import com.latticeengines.playmaker.entitymgr.PlaymakerDaoFactory;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;

@Component("playmakerRecommendationEntityMgr")
public class PlaymakerRecommendationEntityMgrImpl implements PlaymakerRecommendationEntityMgr {

    private static final int MAXIMUM_DESCRIPTION_LENGTH = 255;

    private static final Logger log = LoggerFactory.getLogger(PlaymakerRecommendationEntityMgrImpl.class);

    @Inject
    private PlaymakerDaoFactory daoFactory;

    @Override
    public Map<String, Object> getRecommendations(String tenantName, String lookupSource, long start, int offset,
            int maximum, int syncDestination, List<String> playIds, Map<String, String> orgInfo,
            Map<String, String> appId) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> recommendations = dao.getRecommendations(start, offset, maximum, syncDestination,
                playIds, orgInfo, appId);

        truncateDescriptionLength(recommendations);

        return wrapResult(recommendations);
    }

    @Override
    public Map<String, Object> getRecommendationCount(String tenantName, String lookupSource, long start,
            int syncDestination, List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);
        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getRecommendationCount(start, syncDestination, playIds, orgInfo, appId));
        return result;
    }

    @Override
    public Map<String, Object> getPlays(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> plays = dao.getPlays(start, offset, maximum, playgroupIds, syncDestination, orgInfo);

        truncateDescriptionLength(plays);

        return wrapResult(plays);
    }

    void truncateDescriptionLength(List<Map<String, Object>> maps) {
        String postfix = "...";
        if (CollectionUtils.isEmpty(maps)) {
            return;
        }
        for (Map<String, Object> map : maps) {
            String description = (String) map.get(PlaymakerConstants.Description);
            if (description != null) {
                if (description.length() > MAXIMUM_DESCRIPTION_LENGTH) {
                    String sub_description = description.substring(0, MAXIMUM_DESCRIPTION_LENGTH - postfix.length()) //
                            + postfix;
                    map.put(PlaymakerConstants.Description, sub_description);
                }
            }
        }
    }

    @Override
    public Map<String, Object> getPlayCount(String tenantName, String lookupSource, long start,
            List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getPlayCount(start, playgroupIds, syncDestination, orgInfo));
        return result;
    }

    @Override
    public Map<String, Object> getAccountExtensions(String tenantName, String lookupSource, Long start, int offset,
            int maximum, List<String> accountIds, String filterBy, Long recStart, String columns,
            boolean hasSfdcContactId, Map<String, String> orgInfo) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> extensions = dao.getAccountExtensions(start, offset, maximum, accountIds, filterBy,
                recStart, columns, hasSfdcContactId, orgInfo);
        return wrapResult(extensions);
    }

    @Override
    public Map<String, Object> getAccountExtensionCount(String tenantName, String lookupSource, Long start,
            List<String> accountIds, String filterBy, Long recStart, Map<String, String> orgInfo) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getAccountExtensionCount(start, accountIds, filterBy, recStart, orgInfo));
        return result;
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        return dao.getAccountExtensionSchema(tenantName);
    }

    @Override
    public Map<String, Object> getAccountExtensionColumnCount(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getAccountExtensionColumnCount(tenantName));
        return result;
    }

    @Override
    public Map<String, Object> getContacts(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<String> contactIds, List<String> accountIds, Long recStart, List<String> playIds, Map<String, String> orgInfo,
            Map<String, String> appId) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> contacts = dao.getContacts(start, offset, maximum, contactIds, accountIds, recStart, playIds,
                orgInfo, appId);
        Map<String, Object> result = wrapResult(contacts);
        log.debug("get contacts: {}", result);
        return result;
    }

    @Override
    public Map<String, Object> getContactCount(String tenantName, String lookupSource, long start,
            List<String> contactIds, List<String> accountIds, Long recStart, List<String> playIds, Map<String, String> orgInfo,
            Map<String, String> appId) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getContactCount(start, contactIds, accountIds, recStart, playIds, orgInfo, appId));
        return result;
    }

    @Override
    public Map<String, Object> getContactExtensions(String tenantName, String lookupSource, long start, int offset,
            int maximum, List<String> accountIds, Long recStart, Map<String, String> orgInfo,
            Map<String, String> appId) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> extensions = dao.getContactExtensions(start, offset, maximum, accountIds, recStart,
                orgInfo, appId);
        return wrapResult(extensions);
    }

    @Override
    public Map<String, Object> getContactExtensionCount(String tenantName, String lookupSource, long start,
            List<String> accountIds, Long recStart, Map<String, String> orgInfo, Map<String, String> appId) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getContactExtensionCount(start, accountIds, recStart, orgInfo, appId));
        return result;
    }

    @Override
    public List<Map<String, Object>> getContactExtensionSchema(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);
        return dao.getContactExtensionSchema(tenantName);
    }

    @Override
    public Map<String, Object> getContactExtensionColumnCount(String tenantName, String lookupSource) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        Map<String, Object> result = new HashMap<>();
        result.put(COUNT_KEY, dao.getContactExtensionColumnCount(tenantName));
        return result;
    }

    @Override
    public Map<String, Object> getPlayValues(String tenantName, String lookupSource, long start, int offset,
            int maximum, List<Integer> playgroupIds) {
        PlaymakerRecommendationDao dao = daoFactory.getRecommendationDao(tenantName, lookupSource);

        List<Map<String, Object>> values = dao.getPlayValues(start, offset, maximum, playgroupIds);
        return wrapResult(values);
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
            if (records.get(0).containsKey(RECOMMENDATION_DATE)) {
                result.put(START_KEY, records.get(0).get(RECOMMENDATION_DATE));
                result.put(END_KEY, records.get(records.size() - 1).get(RECOMMENDATION_DATE));
            }
            else {
                result.put(START_KEY, records.get(0).get(LAST_MODIFIATION_DATE_KEY));
                result.put(END_KEY, records.get(records.size() - 1).get(LAST_MODIFIATION_DATE_KEY));
            }
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
