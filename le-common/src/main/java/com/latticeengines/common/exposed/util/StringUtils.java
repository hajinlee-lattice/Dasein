package com.latticeengines.common.exposed.util;

import com.google.common.base.Strings;

public class StringUtils {

    public static boolean objectIsNullOrEmptyString(Object obj) {
        if (obj == null) {
            return true;
        } else {
            String value = String.valueOf(obj);
            return Strings.isNullOrEmpty(value);
        }
    }

}
