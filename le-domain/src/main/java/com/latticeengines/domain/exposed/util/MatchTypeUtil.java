package com.latticeengines.domain.exposed.util;

import org.apache.commons.lang.StringUtils;

public class MatchTypeUtil {
    private static final String DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING = "1.";

    private static final String DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING = "2.";

    public static boolean isValidForRTSBasedMatch(String version) {
        if (StringUtils.isEmpty(version)
                || version.trim().startsWith(DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING)) {
            return true;
        }

        return false;
    }

    public static boolean isValidForAccountMasterBasedMatch(String version) {
        if (!StringUtils.isEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING)) {
            return true;
        }

        return false;
    }

}
