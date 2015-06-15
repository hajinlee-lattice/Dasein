package com.latticeengines.playmaker.entitymgr;

import java.util.List;
import java.util.Map;

public interface PlaymakerRecommendationEntityMgr {

    public static final String ID_KEY = "ID";
    public static final String RECORDS_KEY = "records";
    public static final String END_KEY = "end";
    public static final String START_KEY = "start";
    public static final String LAST_MODIFIATION_DATE_KEY = "LastModificationDate";

    List<Map<String, Object>> getAccountExtensionSchema(String tenantName);

    Map<String, Object> getRecommendations(String tenantName, long start, int offset, int maximum);

    Map<String, Object> getPlays(String tenantName, long start, int offset, int maximum);

    Map<String, Object> getAccountextensions(String tenantName, long start, int offset, int maximum);

    Map<String, Object> getPlayValues(String tenantName, long start, int offset, int maximum);

    int getRecommendationCount(String tenantName, long start);

    int getPlayCount(String tenantName, long start);

    int getAccountextensionCount(String tenantName, long start);

    int getPlayValueCount(String tenantName, long start);

}
