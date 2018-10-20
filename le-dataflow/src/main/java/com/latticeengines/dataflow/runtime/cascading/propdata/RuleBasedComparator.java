package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.commons.lang3.StringUtils;

public class RuleBasedComparator {

    public static int preferBooleanValuedStringAsTrue(String checking, String checked) {
        if (StringUtils.isNotEmpty(checking)
                && (checking.equalsIgnoreCase("Y") || checking.equalsIgnoreCase("YES")
                        || checking.equalsIgnoreCase("1") || checking.equalsIgnoreCase("TRUE"))
                && (StringUtils.isEmpty(checked) || (!checked.equalsIgnoreCase("Y")
                        && !checked.equalsIgnoreCase("YES") && !checked.equalsIgnoreCase("1")
                        && !checked.equalsIgnoreCase("TRUE")))) {
            return 1;
        } else if (StringUtils.isNotEmpty(checked)
                && (checked.equalsIgnoreCase("Y") || checked.equalsIgnoreCase("YES")
                        || checked.equalsIgnoreCase("1") || checked.equalsIgnoreCase("TRUE"))
                && (StringUtils.isEmpty(checking) || (!checking.equalsIgnoreCase("Y")
                        && !checking.equalsIgnoreCase("YES") && !checking.equalsIgnoreCase("1")
                        && !checking.equalsIgnoreCase("TRUE")))) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int preferNotEmptyString(String checking, String checked) {
        if (StringUtils.isNotEmpty(checking) && StringUtils.isEmpty(checked)) {
            return 1;
        } else if (StringUtils.isNotEmpty(checked) && StringUtils.isEmpty(checking)) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int preferLargerLongWithThreshold(Long checking, Long checked, long threshold,
            long gap) {
        if (checking != null && checking >= threshold
                && (checked == null || checking.longValue() >= (checked.longValue() + gap))) {
            return 1;
        } else if (checked != null && checked >= threshold
                && (checking == null || checked.longValue() >= (checking.longValue() + gap))) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int preferEqualStrings(String checking1, String checking2, String checked1,
            String checked2) {
        if (StringUtils.isNotEmpty(checking1) && StringUtils.isNotEmpty(checking2)
                && checking1.equals(checking2) && (StringUtils.isEmpty(checked1)
                        || StringUtils.isEmpty(checked2) || !checked1.equals(checked2))) {
            return 1;
        } else if (StringUtils.isNotEmpty(checked1) && StringUtils.isNotEmpty(checked2)
                && checked1.equals(checked2) && (StringUtils.isEmpty(checking1)
                        || StringUtils.isEmpty(checking2) || !checking1.equals(checking2))) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int preferLargerLong(Long checking, Long checked) {
        if (checking != null && (checked == null || checking.longValue() > checked.longValue())) {
            return 1;
        } else if (checked != null
                && (checking == null || checked.longValue() > checking.longValue())) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int preferExpectedString(String checking, String checked, String expected) {
        if (StringUtils.isNotEmpty(checking) && checking.equalsIgnoreCase(expected)
                && (StringUtils.isEmpty(checked) || !checked.equalsIgnoreCase(expected))) {
            return 1;
        } else if (StringUtils.isNotEmpty(checked) && checked.equalsIgnoreCase(expected)
                && (StringUtils.isEmpty(checking) || !checking.equalsIgnoreCase(expected))) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int preferLargerInteger(Integer checking, Integer checked) {
        if (checking != null && (checked == null || checking.intValue() > checked.intValue())) {
            return 1;
        } else if (checked != null
                && (checking == null || checked.intValue() > checking.intValue())) {
            return -1;
        } else {
            return 0;
        }
    }
}
