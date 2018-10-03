package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import org.apache.commons.lang3.StringUtils;

public class TypeConversionUtil {

    public static String convertAnyToString(Object fieldVal) {
        try {
            if (fieldVal != null) {
                return String.valueOf(fieldVal);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public static Integer convertAnyToInt(String field, Object fieldVal) {
        try {
            if (fieldVal != null) {
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
            return null;
        }
        catch (Exception e) {
            throw new UnsupportedOperationException("The target field : " + field + "with value : " + fieldVal
                    + " cannot be casted to required type int", e);
        }
    }

    public static Long convertAnyToLong(String field, Object fieldVal) {
        try {
            if (fieldVal != null) {
                Long longVal;
                String strFieldVal = convertAnyToString(fieldVal);
                if (StringUtils.countMatches(strFieldVal, ".") == 1) {
                    longVal = Long.parseLong(strFieldVal.substring(0, strFieldVal.indexOf(".")));
                } else {
                    longVal = Long.parseLong(strFieldVal);
                }
                return longVal;
            }
            return null;
        } catch (Exception e) {
            throw new UnsupportedOperationException("The target field : " + field + "with value : " + fieldVal
                    + " cannot be casted to required type long", e);
        }
    }

    public static Double convertAnyToDouble(String field, Object fieldVal) {
        try {
            if (fieldVal != null) {
                return Double.parseDouble(convertAnyToString(fieldVal));
            }
            return null;
        } catch (Exception e) {
            throw new UnsupportedOperationException("The target field : " + field + " with value : " + fieldVal
                    + " cannot be casted to required type double", e);
        }
    }

    public static Boolean convertAnyToBoolean(String field, Object fieldVal) {
        try {
            if (fieldVal != null) {
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
            return null;
        } catch (Exception e) {
            throw new UnsupportedOperationException("The target field : " + field + " with value : " + fieldVal
                    + " cannot be casted to required type boolean", e);
        }
    }

}
