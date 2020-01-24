package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import org.apache.commons.lang3.StringUtils;
import org.kitesdk.shaded.com.google.common.base.Preconditions;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

public final class RuleBasedComparator {

    protected RuleBasedComparator() {
        throw new UnsupportedOperationException();
    }

    private static final String VAL_Y = DataCloudConstants.ATTR_VAL_Y;
    private static final String VAL_YES = DataCloudConstants.ATTR_VAL_YES;
    private static final String VAL_1 = DataCloudConstants.ATTR_VAL_1;
    private static final String VAL_TRUE = DataCloudConstants.ATTR_VAL_TRUE;

    /********************************
     * Rule Comparator for String
     ********************************/

    /**
     * If candidate is true-valued string but compareTo is not, return 1 (candidate wins)
     * If candidate is not true-valued string while compareTo is, return -1 (compareTo wins)
     * Otherwise, return 0 (tie)
     * 
     * @param candidate
     * @param compareTo
     * @return
     */
    public static int preferBooleanValuedStringAsTrue(String candidate, String compareTo) {
        if (isBooleanValuedStringAsTrue(candidate) && !isBooleanValuedStringAsTrue(compareTo)) {
            return 1;
        } else if (isBooleanValuedStringAsTrue(compareTo) && !isBooleanValuedStringAsTrue(candidate)) {
            return -1;
        } else {
            return 0;
        }
    }

    private static boolean isBooleanValuedStringAsTrue(String str) {
        if (VAL_Y.equalsIgnoreCase(str) || VAL_YES.equalsIgnoreCase(str) || VAL_1.equalsIgnoreCase(str)
                || VAL_TRUE.equalsIgnoreCase(str)) {
            return true;
        }
        return false;
    }

    /**
     * If candidate is not blank string, while compareTo is, return 1 (candidate wins)
     * If candidate is blank string, while compareTo is not, return -1 (compareTo wins)
     * Otherwise, return 0 (tie)
     * 
     * @param candidate
     * @param compareTo
     * @return
     */
    public static int preferNonBlankString(String candidate, String compareTo) {
        if (StringUtils.isNotBlank(candidate) && StringUtils.isBlank(compareTo)) {
            return 1;
        } else if (StringUtils.isNotBlank(compareTo) && StringUtils.isBlank(candidate)) {
            return -1;
        } else {
            return 0;
        }
    }

    /**
     * If candidate equals expected, while compareTo does not, return 1 (candidate wins)
     * If candidate does not equal to expected, while compareTo does, return -1 (compareTo wins)
     * Otherwise, return 0 (tie)
     * 
     * @param candidate
     * @param compareTo
     * @param expected
     * @param caseSensitive
     * @return
     */
    public static int preferExpectedString(String candidate, String compareTo, @NotNull String expected,
            boolean caseInsensitive) {
        Preconditions.checkNotNull(expected);
        if (caseInsensitive) {
            if (expected.equalsIgnoreCase(candidate) && !expected.equalsIgnoreCase(compareTo)) {
                return 1;
            } else if (!expected.equalsIgnoreCase(candidate) && expected.equalsIgnoreCase(compareTo)) {
                return -1;
            } else {
                return 0;
            }
        } else {
            if (expected.equals(candidate) && !expected.equals(compareTo)) {
                return 1;
            } else if (!expected.equals(candidate) && expected.equals(compareTo)) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    /**
     * If candidate1 equals candidate2, while compareTo1 does not equal to compareTo2, return 1 (candidate wins)
     * If candidate1 does not equal to candidate2, while compareTo1 equals to compareTo2, return -1 (compareTo wins)
     * Otherwise, return 0 (tie)
     * 
     * @param candidate1
     * @param candidate2
     * @param compareTo1
     * @param compareTo2
     * @param caseSensitive
     * @param trim
     * @return
     */
    public static int preferEqualStrings(String candidate1, String candidate2, String compareTo1, String compareTo2,
            boolean caseInsensitive, boolean trim) {
        if (isEqualStrings(candidate1, candidate2, caseInsensitive, trim)
                && !isEqualStrings(compareTo1, compareTo2, caseInsensitive, trim)) {
            return 1;
        } else if (!isEqualStrings(candidate1, candidate2, caseInsensitive, trim)
                && isEqualStrings(compareTo1, compareTo2, caseInsensitive, trim)) {
            return -1;
        } else {
            return 0;
        }
    }

    private static boolean isEqualStrings(String str1, String str2, boolean caseInsensitive, boolean trim) {
        if (str1 == null && str2 == null) {
            return true;
        }
        if (str1 == null || str2 == null) {
            return false;
        }
        if (trim) {
            str1 = str1.trim();
            str2 = str2.trim();
        }
        if (caseInsensitive) {
            return str1.equalsIgnoreCase(str2);
        } else {
            return str1.equals(str2);
        }
    }

    /********************************
     * Rule Comparator for Long
     ********************************/

    /**
     * If candidate is not null AND candidate >= threshold AND (compareTo is
     * null OR candidate > compareTo + gap), return 1 (candidate wins)
     * 
     * If compareTo is not null AND compareTo >= threshold AND (candidate is
     * null OR compareTo > candidate + gap), return -1 (compareTo wins)
     * 
     * Otherwise return 0 (tie)
     * 
     * @param candidate
     * @param compareTo
     * @param threshold
     * @param gap
     * @return
     */
    public static int preferLargerLongWithThreshold(Long candidate, Long compareTo, long threshold,
            long gap) {
        Preconditions.checkArgument(gap >= 0);
        if (candidate != null && candidate >= threshold
                && (compareTo == null || candidate.longValue() > (compareTo.longValue() + gap))) {
            return 1;
        } else if (compareTo != null && compareTo >= threshold
                && (candidate == null || compareTo.longValue() > (candidate.longValue() + gap))) {
            return -1;
        } else {
            return 0;
        }
    }

    /**
     * If candidate is not null AND (compareTo is null OR candidate >
     * compareTo), return 1 (candidate wins)
     * 
     * If compareTo is not null AND (candidate is null OR compareTo >
     * candidate), return -1 (compareTo wins)
     * 
     * Otherwise return 0 (tie)
     * 
     * @param candidate
     * @param compareTo
     * @return
     */
    public static int preferLargerLong(Long candidate, Long compareTo) {
        if (candidate != null && (compareTo == null || candidate.longValue() > compareTo.longValue())) {
            return 1;
        } else if (compareTo != null
                && (candidate == null || compareTo.longValue() > candidate.longValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    /********************************
     * Rule Comparator for Integer
     ********************************/

    /**
     * If candidate < compareTo, or candidate is not null while compareTo is null, return 1 (candidate wins)
     * If compareTo < candidate, or compareTo is not null while candidate is null, return -1 (compareTo wins)
     * Otherwise, return 0 (tie)
     * ATTENTION: not null beats null
     * 
     * @param candidate
     * @param compareTo
     * @return
     */
    public static int preferSmallerInteger(Integer candidate, Integer compareTo) {
        if (candidate != null && (compareTo == null || candidate.intValue() < compareTo.intValue())) {
            return 1;
        } else if (compareTo != null && (candidate == null || compareTo.intValue() < candidate.intValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    /**
     * If candidate > compareTo, or candidate is not null while compareTo is null, return 1 (candidate wins)
     * If compareTo > candidate, or compareTo is not null while candidate is null, return -1 (compareTo wins)
     * Otherwise, return 0 (tie)
     * ATTENTION: not null beats null
     * 
     * @param candidate
     * @param compareTo
     * @return
     */
    public static int preferLargerInteger(Integer candidate, Integer compareTo) {
        if (candidate != null && (compareTo == null || candidate.intValue() > compareTo.intValue())) {
            return 1;
        } else if (compareTo != null
                && (candidate == null || compareTo.intValue() > candidate.intValue())) {
            return -1;
        } else {
            return 0;
        }
    }

}
