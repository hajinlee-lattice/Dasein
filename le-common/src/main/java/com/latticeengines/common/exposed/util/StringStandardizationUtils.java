package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringStandardizationUtils {
    private static final Logger log = LoggerFactory.getLogger(StringStandardizationUtils.class);

    private Character[] removed = {};
    private Character[] replacedBySpace = {};
    private String[][] replaced = {};
    private static final int LATTICE_ID_LENGTH = 13;
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

    public static String cleanNullString(String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        if ("null".equalsIgnoreCase(str.trim()) || "none".equalsIgnoreCase(str.trim())) {
            return null;
        }
        return str;
    }

    public static String getStandardDuns(String duns) {
        if (StringUtils.isBlank(duns)) {
            return null;
        }
        duns = duns.replaceAll("[^\\d]", "");
        if (StringUtils.isEmpty(duns) || duns.length() > 9) {
            return null;
        }
        if (duns.length() < 9) {
            duns = ("000000000" + duns).substring(duns.length());
        }
        return duns;
    }

    public static String getStandardizedInputLatticeID(String latticeID) {
        if (StringUtils.isBlank(latticeID) || latticeID.trim().startsWith("-")
                || latticeID.startsWith("+")) {
            return null;
        }

        try {
            Long id = Long.valueOf(latticeID.trim());
            String latticeIdAsString = id.toString();
            return (latticeIdAsString.length() > LATTICE_ID_LENGTH) ? null : latticeIdAsString;
        } catch (NumberFormatException exc) {
            return null;
        }
    }

    // TODO(jwinter): Complete implementation of this function according to PM requirements.
    public static String getStandardizedSystemId(String systemId) {
        if (StringUtils.isBlank(systemId)) {
            return null;
        }

        // system ID matching are case in-sensitive
        return systemId.trim().toLowerCase();
    }

    public static String getStandardizedOutputLatticeID(String latticeId) {
        if (StringUtils.isBlank(latticeId)) {
            return null;
        }

        try {
            Long latticeIdAsLong = Long.valueOf(latticeId);
            String latticeIdAsString = latticeIdAsLong.toString();
            if (latticeIdAsString.length() > LATTICE_ID_LENGTH) {
                log.error(String.format(
                        "LatticeAccountId %s is too long. Required length is less than or equals to "
                                + "%s, but actual is %s",
                        latticeId, String.valueOf(LATTICE_ID_LENGTH), latticeId.length()));
                return null;
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < (LATTICE_ID_LENGTH - latticeIdAsString.length()); i++) {
                sb.append("0");
            }

            return sb.append(latticeIdAsString).toString();
        } catch (NumberFormatException exc) {
            log.error(String.format("LatticeId %s is not in numeric format.", latticeId));
            return null;
        }
    }

    String getStandardStringInternal(String str) {
        try {
            if (cleanNullString(str) == null) {
                return null;
            }
            Set<Character> removedSet = new HashSet<>(getCharactersToRemove());
            Set<Character> replacedBySpaceSet = new HashSet<>(
                    getCharactersToReplaceWithWhiteSpace());
            // Always change to upper case
            StringBuilder sb = new StringBuilder(str.toUpperCase());
            for (int i = 0; i < sb.length(); i++) {
                if (removedSet.contains(sb.charAt(i))) {
                    // Remove specific characters
                    sb.replace(i, i + 1, "");
                    i--;
                } else if (replacedBySpaceSet.contains(sb.charAt(i))) {
                    // Replace specific characters with whitespace
                    sb.replace(i, i + 1, " ");
                }
            }
            String res = sb.toString();
            // Replace specific characters with words
            Map<String, String> replaced = getCharactersToReplaceWithWord();
            for (Map.Entry<String, String> entry : replaced.entrySet()) {
                res = res.replaceAll(entry.getKey(), " " + entry.getValue() + " ");
            }
            // Remove leading and trailing spaces;
            // Replace multiple connected spaces with single space
            res = res.trim().replaceAll("\\s+", " ");
            if (cleanNullString(res) == null) {
                return null;
            }
            return res;
        } catch (Exception ex) {
            log.error("Fail to standardize string " + str, ex);
            return str; // If any exception is caught, return original string
        }
    }

    protected List<Character> getCharactersToRemove() {
        return new ArrayList<>(Arrays.asList(removed));
    }

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
