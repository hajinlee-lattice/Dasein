package com.latticeengines.domain.exposed.util;

import org.apache.commons.lang3.StringUtils;

public class MatchTypeUtil {

    private static final String DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING = "2.";

    public static boolean isValidForAccountMasterBasedMatch(String version) {
        return !StringUtils.isEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING);

    }

}
