package com.latticeengines.playmaker.entitymgr;

import java.util.List;
import java.util.Map;

public interface PlaymakerRecommendationEntityMgr {

    public static final String ID_KEY = "ID";
    public static final String RECORDS_KEY = "records";
    public static final String END_KEY = "endDatetime";
    public static final String START_KEY = "startDatetime";
    public static final String LAST_MODIFIATION_DATE_KEY = "LastModificationDate";
    public static final String COUNT_KEY = "count";

    List<Map<String, Object>> getAccountExtensionSchema(String tenantName, String lookupSource);

    Map<String, Object> getRecommendations(String tenantName, String lookupSource, long start, int offset, int maximum,
            int syncDestination, List<String> playIds);

    Map<String, Object> getPlays(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<Integer> playgroupIds);

    Map<String, Object> getAccountExtensions(String tenantName, String lookupSource, Long start, int offset,
            int maximum, List<String> accountIds, String filterBy, Long recStart, String columns,
            boolean hasSfdcContactId);

    Map<String, Object> getPlayValues(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<Integer> playgroupIds);

    List<Map<String, Object>> getWorkflowTypes(String tenantName, String lookupSource);

    Map<String, Object> getRecommendationCount(String tenantName, String lookupSource, long start, int syncDestination,
            List<String> playIds);

    Map<String, Object> getPlayCount(String tenantName, String lookupSource, long start, List<Integer> playgroupIds);

    Map<String, Object> getAccountextExsionCount(String tenantName, String lookupSource, Long start,
            List<String> accountIds, String filterBy, Long recStart);

    Map<String, Object> getPlayValueCount(String tenantName, String lookupSource, long start,
            List<Integer> playgroupIds);

    Map<String, Object> getAccountExtensionColumnCount(String tenantName, String lookupSource);

    List<Map<String, Object>> getPlayGroups(String tenantName, String lookupSource, long start, int offset,
            int maximum);

    Map<String, Object> getPlayGroupCount(String tenantName, String lookupSource, long start);

    Map<String, Object> getContacts(String tenantName, String lookupSource, long start, int offset, int maximum,
            List<Integer> contactIds, List<Integer> accountIds);

    Map<String, Object> getContactCount(String tenantName, String lookupSource, long start, List<Integer> contactIds,
            List<Integer> accountIds);

    Map<String, Object> getContactExtensionColumnCount(String tenantName, String lookupSource);

    List<Map<String, Object>> getContactExtensionSchema(String tenantName, String lookupSource);

    Map<String, Object> getContactExtensionCount(String tenantName, String lookupSource, long start,
            List<Integer> accountIds);

    Map<String, Object> getContactExtensions(String tenantName, String lookupSource, long start, int offset,
            int maximum, List<Integer> accountIds);

}
