package com.latticeengines.common.exposed.util;

import com.google.common.base.Strings;

public class YarnUtils {

    public static boolean isPrempted(String diagnostics) {
        if (Strings.isNullOrEmpty(diagnostics))
            return false;

        return (diagnostics.contains("-102") && diagnostics.contains("Container preempted by scheduler"));
    }

}