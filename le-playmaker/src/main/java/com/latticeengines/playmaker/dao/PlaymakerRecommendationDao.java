package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.GenericDao;

public interface PlaymakerRecommendationDao extends GenericDao {

    List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<String> playIds);

    int getRecommendationCount(long start, int syncDestination, List<String> playIds);

    List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds);

    int getPlayCount(long start, List<Integer> playgroupIds);

    List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<String> accountIds,
            String filterBy, Long recStart, String columns, boolean hasSfdcContactId);

    int getAccountExtensionCount(long start, List<String> accountIds, String filterBy, Long recStart);

    List<Map<String, Object>> getAccountExtensionSchema();

    int getAccountExtensionColumnCount();

    List<Map<String, Object>> getPlayValues(long start, int offset, int maximum, List<Integer> playgroupIds);

    int getPlayValueCount(long start, List<Integer> playgroupIds);

    List<Map<String, Object>> getWorkflowTypes();

    List<Map<String, Object>> getPlayGroups(long start, int offset, int maximum);

    int getPlayGroupCount(long start);

    List<Map<String, Object>> getContacts(long start, int offset, int maximum, List<Integer> contactIds,
            List<Integer> accountIds);

    int getContactCount(long start, List<Integer> contactIds, List<Integer> accountIds);

    List<Map<String, Object>> getContactExtensionSchema();

    int getContactExtensionCount(long start, List<Integer> contactIds);

    List<Map<String, Object>> getContactExtensions(long start, int offset, int maximum, List<Integer> contactIds);

    int getContactExtensionColumnCount();

}
