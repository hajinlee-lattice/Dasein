package com.latticeengines.common.exposed.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StringUtils {

    public static boolean objectIsNullOrEmptyString(Object obj) {
        if (obj == null) {
            return true;
        } else {
            String value = String.valueOf(obj);
            return org.apache.commons.lang.StringUtils.isBlank(value);
        }
    }

    public static String getStandardString(String str) {
        if (str == null) {
            return null;
        }
        Character[] symbols = { '~', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '+', '=', '{', '}', '[',
                ']', '|', '\\', ':', ';', '\'', '"', '<', '>', ',', '.', '/', '?', '\t' };
        Set<Character> symbolSet = new HashSet<Character>(Arrays.asList(symbols));
        StringBuilder sb = new StringBuilder(str.toUpperCase());
        for (int i = 0; i < sb.length(); i++) {
            if (symbolSet.contains(sb.charAt(i))) {
                sb.replace(i, i + 1, " ");
            }
        }
        String res = sb.toString().trim();
        while (res.indexOf("  ") >= 0) {
            res = res.replace("  ", " ");
        }
        return res;
    }

}