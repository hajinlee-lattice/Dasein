package com.latticeengines.common.exposed.util;


public class StringUtils {

    public static boolean objectIsNullOrEmptyString(Object obj) {
        if (obj == null) {
            return true;
        } else {
            String value = String.valueOf(obj);
            return org.apache.commons.lang.StringUtils.isBlank(value);
        }
    }

}
