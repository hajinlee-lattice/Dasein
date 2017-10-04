package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.GenericDao;

public interface PlaymakerRecommendationDao extends GenericDao {

    List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<String> playIds);

    long getRecommendationCount(long start, int syncDestination, List<String> playIds);

    List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds);

    long getPlayCount(long start, List<Integer> playgroupIds);

    List<Map<String, Object>> getAccountExtensions(Long start, int offset, int maximum, List<String> accountIds,
            String filterBy, Long recStart, String columns, boolean hasSfdcContactId);

    long getAccountExtensionCount(Long start, List<String> accountIds, String filterBy, Long recStart);

    List<Map<String, Object>> getAccountExtensionSchema();

    long getAccountExtensionColumnCount();

    List<Map<String, Object>> getPlayValues(long start, int offset, int maximum, List<Integer> playgroupIds);

    long getPlayValueCount(long start, List<Integer> playgroupIds);

    List<Map<String, Object>> getWorkflowTypes();

    List<Map<String, Object>> getPlayGroups(long start, int offset, int maximum);

    long getPlayGroupCount(long start);

    List<Map<String, Object>> getContacts(long start, int offset, int maximum, List<Integer> contactIds,
            List<Integer> accountIds);

    long getContactCount(long start, List<Integer> contactIds, List<Integer> accountIds);

    List<Map<String, Object>> getContactExtensionSchema();

    long getContactExtensionCount(long start, List<Integer> contactIds);

    List<Map<String, Object>> getContactExtensions(long start, int offset, int maximum, List<Integer> contactIds);

    long getContactExtensionColumnCount();

}
