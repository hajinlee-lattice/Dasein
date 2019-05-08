package com.latticeengines.playmaker.entitymgr;

import java.util.List;
import java.util.Map;

public interface PlaymakerRecommendationEntityMgr {

    String ID_KEY = "ID";
    String RECORDS_KEY = "records";
    String END_KEY = "endDatetime";
    String START_KEY = "startDatetime";
    String LAST_MODIFIATION_DATE_KEY = "LastModificationDate";
    String COUNT_KEY = "count";
    String RECOMMENDATION_DATE = "recommendationDate";

    List<Map<String, Object>> getAccountExtensionSchema(String tenantName, String lookupSource);

    Map<String, Object> getRecommendations(String tenantName, String lookupSource, long start, int offset, int maximum,
            int syncDestination, List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId);

    Map<String, Object> getPlays(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo);

    Map<String, Object> getAccountExtensions(String tenantName, String lookupSource, Long start, int offset,
            int maximum, List<String> accountIds, String filterBy, Long recStart, String columns,
            boolean hasSfdcContactId, Map<String, String> orgInfo);

    Map<String, Object> getPlayValues(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<Integer> playgroupIds);

    List<Map<String, Object>> getWorkflowTypes(String tenantName, String lookupSource);

    Map<String, Object> getRecommendationCount(String tenantName, String lookupSource, long start, int syncDestination,
            List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId);

    Map<String, Object> getPlayCount(String tenantName, String lookupSource, long start, List<Integer> playgroupIds,
            int syncDestination, Map<String, String> orgInfo);

    Map<String, Object> getAccountExtensionCount(String tenantName, String lookupSource, Long start,
            List<String> accountIds, String filterBy, Long recStart, Map<String, String> orgInfo);

    Map<String, Object> getPlayValueCount(String tenantName, String lookupSource, long start,
            List<Integer> playgroupIds);

    Map<String, Object> getAccountExtensionColumnCount(String tenantName, String lookupSource);

    List<Map<String, Object>> getPlayGroups(String tenantName, String lookupSource, long start, int offset,
            int maximum);

    Map<String, Object> getPlayGroupCount(String tenantName, String lookupSource, long start);

    Map<String, Object> getContacts(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<String> contactIds, List<String> accountIds, Long recStart, List<String> playIds, Map<String, String> orgInfo,
            Map<String, String> appId);

    Map<String, Object> getContactCount(String tenantName, String lookupSource, long start, List<String> contactIds,
            List<String> accountIds, Long recStart, List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId);

    Map<String, Object> getContactExtensionColumnCount(String tenantName, String lookupSource);

    List<Map<String, Object>> getContactExtensionSchema(String tenantName, String lookupSource);

    Map<String, Object> getContactExtensionCount(String tenantName, String lookupSource, long start,
            List<String> accountIds, Long recStart, Map<String, String> orgInfo, Map<String, String> appId);

    Map<String, Object> getContactExtensions(String tenantName, String lookupSource, long start, int offset,
            int maximum, List<String> accountIds, Long recStart, Map<String, String> orgInfo,
            Map<String, String> appId);

}
