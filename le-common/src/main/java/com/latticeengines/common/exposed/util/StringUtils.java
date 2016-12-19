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
        try {
            if (org.apache.commons.lang.StringUtils.isEmpty(str)) {
                return null;
            }
            Character[] removed = { '~', '@', '#', '$', '%', '^', '*', '+', '=', '\'', '"', '<', '>', '’' };
            Character[] replacedBySpace = { '&', '-', '_', '|', '\\', '/', '\t', '?', ';', ':', ',', '(', ')', '{', '}',
                    '[', ']', '.' };
            Set<Character> removedSet = new HashSet<Character>(Arrays.asList(removed));
            Set<Character> replacedBySpaceSet = new HashSet<Character>(Arrays.asList(replacedBySpace));
            StringBuilder sb = new StringBuilder(str.toUpperCase());
            for (int i = 0; i < sb.length(); i++) {
                if (removedSet.contains(sb.charAt(i))) {
                    sb.replace(i, i + 1, "");
                    i--;
                } else if (replacedBySpaceSet.contains(sb.charAt(i))) {
                    sb.replace(i, i + 1, " ");
                }
            }
            String res = sb.toString().trim();
            while (res.indexOf("  ") >= 0) {
                res = res.replace("  ", " ");
            }
            return res;
        } catch (Exception ex) {
            ex.printStackTrace();
            return str;
        }
    }

}