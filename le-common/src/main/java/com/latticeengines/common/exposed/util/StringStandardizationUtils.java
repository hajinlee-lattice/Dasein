package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import edu.emory.mathcs.backport.java.util.Arrays;

public class StringStandardizationUtils {

    private Character[] removed = {};
    private Character[] replacedBySpace = {};
    private String[][] replaced = {};
    private static StringStandardizationUtils singletonUtil = new StringStandardizationUtils();

    public static boolean objectIsNullOrEmptyString(Object obj) {
        if (obj == null) {
            return true;
        } else {
            String value = String.valueOf(obj);
            return StringUtils.isBlank(value);
        }
    }

    public static String getStandardString(String str) {
        return singletonUtil.getStandardStringInternal(str);
    }

    protected String getStandardStringInternal(String str) {
        try {
            if (StringUtils.isEmpty(str)) {
                return null;
            }
            Set<Character> removedSet = new HashSet<Character>(getCharactersToRemove());
            Set<Character> replacedBySpaceSet = new HashSet<Character>(getCharactersToReplaceWithWhiteSpace());
            StringBuilder sb = new StringBuilder(str.toUpperCase());    // Always change to upper case
            for (int i = 0; i < sb.length(); i++) {
                if (removedSet.contains(sb.charAt(i))) {
                    sb.replace(i, i + 1, "");   // Remove specific characters
                    i--;
                } else if (replacedBySpaceSet.contains(sb.charAt(i))) {
                    sb.replace(i, i + 1, " ");  // Replace specific characters with whitespace
                }
            }
            String res = sb.toString();
            Map<String, String> replaced = getCharactersToReplaceWithWord();
            for (Map.Entry<String, String> entry : replaced.entrySet()) {   // Replace specific characters with words
                res = res.replaceAll(entry.getKey(), " " + entry.getValue() + " ");
            }
            res = res.trim(); // Remove leading and trailing spaces
            while (res.indexOf("  ") >= 0) {    // Replace multiple connected spaces with single space
                res = res.replace("  ", " ");
            }
            return res;
        } catch (Exception ex) {
            ex.printStackTrace();
            return str; // If any exception is caught, return original string
        }
    }

    @SuppressWarnings("unchecked")
    protected List<Character> getCharactersToRemove() {
        return new ArrayList<Character>(Arrays.asList(removed));
    }

    @SuppressWarnings("unchecked")
    protected List<Character> getCharactersToReplaceWithWhiteSpace() {
        return new ArrayList<Character>(Arrays.asList(replacedBySpace));
    }

    protected Map<String, String> getCharactersToReplaceWithWord() {
        Map<String, String> map = new HashMap<String, String>();
        for (String[] entry : replaced) {
            map.put(entry[0], entry[1]);
        }
        return map;
    }

}