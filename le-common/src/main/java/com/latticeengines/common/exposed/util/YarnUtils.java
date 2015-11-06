package com.latticeengines.common.exposed.util;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;

import com.google.common.base.Strings;

public class YarnUtils {

    public static boolean isPrempted(String diagnostics) {
        if (Strings.isNullOrEmpty(diagnostics))
            return false;

        return (diagnostics.contains("-102") && diagnostics.contains("Container preempted by scheduler"));
    }

    public static ApplicationId appIdFromString(String appId) {
        String[] tokens = appId.split("_");
        return ApplicationIdPBImpl.newInstance(Long.parseLong(tokens[1]), Integer.parseInt(tokens[2]));
    }

}