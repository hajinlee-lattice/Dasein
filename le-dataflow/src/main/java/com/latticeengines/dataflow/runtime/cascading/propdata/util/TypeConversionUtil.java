package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import org.apache.commons.lang3.StringUtils;

public class TypeConversionUtil {

    public static String convertAnyToString(Object fieldVal) {
        return String.valueOf(fieldVal);
    }

    public static Integer convertAnyToInt(Object fieldVal) {
        Integer intVal;
        String strFieldVal = convertAnyToString(fieldVal);
        if (StringUtils.countMatches(strFieldVal, ".") == 1) {
            intVal = Integer.parseInt(strFieldVal.substring(0, strFieldVal.indexOf(".")));
        } else if (strFieldVal.equalsIgnoreCase("true") || strFieldVal.equalsIgnoreCase("false")) {
            intVal = Boolean.valueOf(strFieldVal) ? 1 : 0;
        } else {
            intVal = Integer.parseInt(strFieldVal);
        }
        return intVal;
    }

    public static Integer convertStringToInt(String fieldVal) {
        return Integer.parseInt(fieldVal);
    }

    public static Long convertAnyToLong(Object fieldVal) {
        Long longVal;
        String strFieldVal = convertAnyToString(fieldVal);
        if (StringUtils.countMatches(strFieldVal, ".") == 1) {
            longVal = Long.parseLong(strFieldVal.substring(0, strFieldVal.indexOf(".")));
        } else {
            longVal = Long.parseLong(strFieldVal);
        }
        return longVal;
    }

    public static Long convertStringToLong(String fieldVal) {
        return Long.parseLong(fieldVal);
    }

    public static Double convertAnyToDouble(Object fieldVal) {
        return Double.parseDouble(convertAnyToString(fieldVal));
    }

    public static Boolean convertAnyToBoolean(Object fieldVal) {
        String strFieldVal = convertAnyToString(fieldVal);
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

    public static Boolean convertStringToBoolean(String fieldVal) {
        if (StringUtils.isEmpty(fieldVal)) {
        } else if (fieldVal.equalsIgnoreCase("Y") || fieldVal.equalsIgnoreCase("YES")
                || fieldVal.equalsIgnoreCase("TRUE") || fieldVal.equalsIgnoreCase("1")) {
            return Boolean.TRUE;
        } else if (fieldVal.equalsIgnoreCase("N") || fieldVal.equalsIgnoreCase("NO")
                || fieldVal.equalsIgnoreCase("FALSE") || fieldVal.equalsIgnoreCase("0")) {
            return Boolean.FALSE;
        }
        return null;
    }

}
