package com.latticeengines.transform.v2_0_25.common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;

public class DSUtils {

    private static HashSet<String> unusualCharacterSet = new HashSet<String>((Arrays.asList("!", "@", "#", "\"", "%",
            "$", "`", "}", "+", "*", "\\", "^", "~", "_", "{", ":", "=", "<", "?", ">")));

    private static HashSet<String> badSet = new HashSet<String>((Arrays.asList("none", "no", "not", "delete", "asd",
            "sdf", "unknown", "undisclosed", "null", "don", "abc", "xyz", "nonname", "nocompany")));

    public static Boolean hasUnUsualChar(String s) {
        if (StringUtils.isEmpty(s))
            return true;

        s = s.trim().toLowerCase();

        // if (Pattern.matches(".*[!@#\"%$`}+*\\^~_{:=<?>].*", s))
        if (Pattern.matches(".*[!@#\"%$`}+*\\^~_{=<?>].*", s))
            return true;

        if (Pattern.matches("(^|\\s+)[\\[]*(none|no|not|delete|asd|sdf|unknown|"
                + "undisclosed|null|don|abc|xyz|nonname|nocompany)($|\\s+)", s))
            return true;

        if (NumberUtils.isNumber(s))
            return true;

        return false;
    }

    @SuppressWarnings("rawtypes")
    public static String valueReturn(String x, LinkedHashMap mappingList) {
        if (StringUtils.isEmpty(x))
            return "Null";

        if (DSUtils.hasUnUsualChar(x))
            // return "other";
            return "";

        x = x.trim().toLowerCase();

        for (Object key : mappingList.keySet()) {
            String[] values = mappingList.get(key).toString().split(",");

            for (String value : values) {
                value = value.trim().toLowerCase();
                // if any(z in y for z in keys): return title
                if (x.contains(value))
                    return key.toString();
            }
        }

        // return "Null";
        return "Other";
    }

}
