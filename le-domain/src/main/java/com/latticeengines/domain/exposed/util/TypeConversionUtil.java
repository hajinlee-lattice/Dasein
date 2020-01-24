package com.latticeengines.domain.exposed.util;

import org.apache.commons.lang3.StringUtils;

public final class TypeConversionUtil {

    protected TypeConversionUtil() {
        throw new UnsupportedOperationException();
    }

    public static String toString(Object fieldVal) {
        if (fieldVal == null) {
            return null;
        }
        return String.valueOf(fieldVal);
    }

    public static Integer toInteger(Object fieldVal) {
        if (fieldVal == null) {
            return null;
        }
        Integer intVal;
        String strFieldVal = toString(fieldVal);
        if (StringUtils.countMatches(strFieldVal, ".") == 1) {
            intVal = Integer.parseInt(strFieldVal.substring(0, strFieldVal.indexOf(".")));
        } else if (strFieldVal.equalsIgnoreCase("true") || strFieldVal.equalsIgnoreCase("false")) {
            intVal = Boolean.valueOf(strFieldVal) ? 1 : 0;
        } else {
            intVal = Integer.parseInt(strFieldVal);
        }
        return intVal;
    }

    public static Long toLong(Object fieldVal) {
        if (fieldVal == null) {
            return null;
        }
        Long longVal;
        String strFieldVal = toString(fieldVal);
        if (StringUtils.countMatches(strFieldVal, ".") == 1) {
            longVal = Long.parseLong(strFieldVal.substring(0, strFieldVal.indexOf(".")));
        } else {
            longVal = Long.parseLong(strFieldVal);
        }
        return longVal;
    }

    public static Double toDouble(Object fieldVal) {
        if (fieldVal == null) {
            return null;
        }
        return Double.parseDouble(toString(fieldVal));
    }

    public static Boolean toBoolean(Object fieldVal) {
        if (fieldVal == null) {
            return null;
        }
        String strFieldVal = toString(fieldVal);
        if (strFieldVal.equalsIgnoreCase("Y") || strFieldVal.equalsIgnoreCase("YES")
                || strFieldVal.equalsIgnoreCase("TRUE") || strFieldVal.equalsIgnoreCase("1")) {
            return Boolean.TRUE;
        } else if (strFieldVal.equalsIgnoreCase("N") || strFieldVal.equalsIgnoreCase("NO")
                || strFieldVal.equalsIgnoreCase("FALSE") || strFieldVal.equalsIgnoreCase("0")) {
            return Boolean.FALSE;
        } else {
            throw new RuntimeException();
        }
    }

}
