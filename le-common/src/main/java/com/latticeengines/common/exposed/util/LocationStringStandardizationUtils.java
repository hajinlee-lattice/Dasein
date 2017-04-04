package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * Used to standardize country/state/phone string
 */
public class LocationStringStandardizationUtils extends StringStandardizationUtils {


    private Character[] removed = { '~', '@', '#', '$', '%', '^', '*', '+', '=', '\'', '<', '>', '’' };
    private Character[] replacedBySpace = { '&', '-', '_', '|', '\\', '/', '\t', '?', ';', ':', ',', '(', ')', '{', '}',
            '[', ']', '.', '"', '\r', '\n' };
    private String[][] replaced = {};
    private static LocationStringStandardizationUtils singletonUtil = new LocationStringStandardizationUtils();

    public static String getStandardString(String str) {
        return singletonUtil.getStandardStringInternal(str);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<Character> getCharactersToRemove() {
        return new ArrayList<>(Arrays.asList(removed));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<Character> getCharactersToReplaceWithWhiteSpace() {
        return new ArrayList<>(Arrays.asList(replacedBySpace));
    }

    @Override
    protected Map<String, String> getCharactersToReplaceWithWord() {
        Map<String, String> map = new HashMap<>();
        for (String[] entry : replaced) {
            map.put(entry[0], entry[1]);
        }
        return map;
    }

}
