package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.GenericDao;

public interface PlaymakerRecommendationDao extends GenericDao {

    List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination, List<Integer> playIds);

    List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds);

    List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<Integer> accountIds);

    List<Map<String, Object>> getAccountExtensionSchema();

    List<Map<String, Object>> getPlayValues(long start, int offset, int maximum, List<Integer> playgroupIds);

    List<Map<String, Object>> getWorkflowTypes();

    int getRecommendationCount(long start, int syncDestination, List<Integer>playIds);

    int getPlayCount(long start, List<Integer> playgroupIds);

    int getAccountExtensionCount(long start, List<Integer> accountIds);

    int getPlayValueCount(long start, List<Integer> playgroupIds);

    int getAccountExtensionColumnCount();

    List<Map<String, Object>> getPlayGroups(long start, int offset, int maximum);

    int getPlayGroupCount(long start);


    List<Map<String, Object>> getContacts(long start, int offset, int maximum, List<Integer> contactIds);

    int getContactCount(long start, List<Integer> contactIds);

    List<Map<String, Object>> getContactExtensionSchema();

    int getContactExtensionCount(long start, List<Integer> contactIds);

    List<Map<String, Object>> getContactExtensions(long start, int offset, int maximum, List<Integer> contactIds);

    int getContactExtensionColumnCount();

}
