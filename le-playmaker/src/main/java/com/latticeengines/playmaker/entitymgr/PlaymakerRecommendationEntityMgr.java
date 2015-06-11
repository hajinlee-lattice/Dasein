package com.latticeengines.playmaker.entitymgr;

import java.util.Map;

public interface PlaymakerRecommendationEntityMgr {

    public static final String ID_KEY = "ID";
    public static final String RECORDS_KEY = "records";
    public static final String END_ID_KEY = "endId";
    public static final String START_ID_KEY = "startId";

    Map<String, Object> getRecommendations(String tenantName, int startId, int size);

    Map<String, Object> getPlays(String tenantName, int startId, int size);

}
