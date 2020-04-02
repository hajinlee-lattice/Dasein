package com.latticeengines.domain.exposed.util;

import org.apache.commons.lang3.StringUtils;

/**
 * General activity store helpers
 */
public class ActivityStoreUtils {

    private ActivityStoreUtils() {
    }

    /**
     * Transform activity store specific pattern into regular expression. This is to
     * support some use cases such as wildcard (*) and can break some regex.
     *
     * @param activityStorePattern
     *            activity store specific pattern (mostly regex)
     * @return transformed regular expression
     */
    public static String modifyPattern(String activityStorePattern) {
        if (StringUtils.isBlank(activityStorePattern)) {
            return activityStorePattern;
        }

        // replace all * (that is not already .*) to .* to support wildcard
        return activityStorePattern.replaceAll("(?<!\\.)\\*", ".*");
    }
}
