package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.GenericDao;

public interface PlaymakerRecommendationDao extends GenericDao {

    List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId);

    long getRecommendationCount(long start, int syncDestination, List<String> playIds, Map<String, String> orgInfo,
            Map<String, String> appId);

    List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds,
            int syncDestination, Map<String, String> orgInfo);

    long getPlayCount(long start, List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo);

    List<Map<String, Object>> getAccountExtensions(Long start, int offset, int maximum, List<String> accountIds,
            String filterBy, Long recStart, String columns, boolean hasSfdcContactId, Map<String, String> orgInfo);

    long getAccountExtensionCount(Long start, List<String> accountIds, String filterBy, Long recStart,
            Map<String, String> orgInfo);

    List<Map<String, Object>> getAccountExtensionSchema(String customerSpace);

    long getAccountExtensionColumnCount(String customerSpace);

    List<Map<String, Object>> getPlayValues(long start, int offset, int maximum, List<Integer> playgroupIds);

    long getPlayValueCount(long start, List<Integer> playgroupIds);

    List<Map<String, Object>> getWorkflowTypes();

    List<Map<String, Object>> getPlayGroups(long start, int offset, int maximum);

    long getPlayGroupCount(long start);

    List<Map<String, Object>> getContacts(long start, int offset, int maximum, List<String> contactIds,
            List<String> accountIds, Long recStart, List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId);

    long getContactCount(long start, List<String> contactIds, List<String> accountIds, Long recStart,
            List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId);

    List<Map<String, Object>> getContactExtensionSchema(String customerSpace);

    long getContactExtensionCount(long start, List<String> contactIds, Long recStart, Map<String, String> orgInfo,
            Map<String, String> appId);

    List<Map<String, Object>> getContactExtensions(long start, int offset, int maximum, List<String> contactIds,
            Long recStart, Map<String, String> orgInfo, Map<String, String> appId);

    long getContactExtensionColumnCount(String customerSpace);

}
