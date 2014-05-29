package com.latticeengines.common.exposed.util;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class YarnUtils {

    public static ApplicationId getApplicationIdFromString(String appId) {
        if (appId == null) {
            return null;
        }
            
        String[] tokens = appId.split("_");
        return ApplicationId.newInstance(Long.parseLong(tokens[1]), Integer.parseInt(tokens[2]));
    }

}