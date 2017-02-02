package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * Used to standardize name/city/street string
 * Leave character '
 */
public class NameStringStandardizationUtils extends StringStandardizationUtils {

    private Character[] removed = { '~', '@', '#', '$', '%', '^', '*', '+', '=', '<', '>', '’' };
    private Character[] replacedBySpace = { '-', '_', '|', '\\', '/', '\t', '?', ';', ':', ',', '(', ')', '{', '}', '[',
            ']', '.', '"' };
    String[][] replaced = { { "&", "AND" } };

    @SuppressWarnings("unchecked")
    @Override
    protected List<Character> getCharactersToRemove() {
        return new ArrayList<Character>(Arrays.asList(removed));
    }

    @SuppressWarnings("unchecked")
    @Override
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
