package com.latticeengines.common.exposed.util;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.common.base.Strings;

public class YarnUtils {

    public static ApplicationId getApplicationIdFromString(String appId) {
        if (appId == null) {
            return null;
        }

        String[] tokens = appId.split("_");
        return ApplicationId.newInstance(Long.parseLong(tokens[1]), Integer.parseInt(tokens[2]));
    }

    public static boolean isPrempted(String diagnostics) {
        if (Strings.isNullOrEmpty(diagnostics))
            return false;

        return (diagnostics.contains("-102") && diagnostics.contains("Container preempted by scheduler"));
    }

}