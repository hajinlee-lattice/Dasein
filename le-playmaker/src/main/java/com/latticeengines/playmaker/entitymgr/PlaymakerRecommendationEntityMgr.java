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

    List<Map<String, Object>> getAccountExtensionSchema(String tenantName);

    Map<String, Object> getRecommendations(String tenantName, long start, int offset, int maximum, int syncDestination, List<Integer> playIds);

    Map<String, Object> getPlays(String tenantName, long start, int offset, int maximum, List<Integer> playgroupIds);

    Map<String, Object> getAccountextensions(String tenantName, long start, int offset, int maximum, List<Integer> accountIds);

    Map<String, Object> getPlayValues(String tenantName, long start, int offset, int maximum, List<Integer> playgroupIds);

    List<Map<String, Object>> getWorkflowTypes(String tenantName);

    Map<String, Object> getRecommendationCount(String tenantName, long start, int syncDestination, List<Integer>playIds);

    Map<String, Object> getPlayCount(String tenantName, long start, List<Integer> playgroupIds);

    Map<String, Object> getAccountextensionCount(String tenantName, long start, List<Integer> accountIds);

    Map<String, Object> getPlayValueCount(String tenantName, long start, List<Integer> playgroupIds);

    Map<String, Object> getAccountExtensionColumnCount(String tenantName);

    List<Map<String, Object>> getPlayGroups(String tenantName, long start, int offset, int maximum);

    Map<String, Object> getPlayGroupCount(String tenantName, long start);

}
