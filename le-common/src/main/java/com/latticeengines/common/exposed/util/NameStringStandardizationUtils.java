package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to standardize name/city/street string
 * Leave character '
 */
public class NameStringStandardizationUtils extends StringStandardizationUtils {

    private Character[] removed = { '~', '@', '#', '$', '%', '^', '*', '+', '=', '<', '>' };
    private Character[] replacedBySpace = { '_', '|', '\\', '/', '\t', '?', ';', ':', '(', ')', '{', '}', '[', ']', '"',
            '\r', '\n' };
    String[][] replaced = { { "&", "AND" } };
    private static NameStringStandardizationUtils singletonUtil = new NameStringStandardizationUtils();

    public static String getStandardString(String str) {
        return singletonUtil.getStandardStringInternal(str);
    }

    @Override
    protected List<Character> getCharactersToRemove() {
        return new ArrayList<>(Arrays.asList(removed));
    }

    @Override
    protected List<Character> getCharactersToReplaceWithWhiteSpace() {
        return new ArrayList<>(Arrays.asList(replacedBySpace));
    }

    protected Map<String, String> getCharactersToReplaceWithWord() {
        Map<String, String> map = new HashMap<>();
        for (String[] entry : replaced) {
            map.put(entry[0], entry[1]);
        }
        return map;
    }
}
