package com.latticeengines.domain.exposed.propdata.match;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.JsonUtils;

public class MatchKeyUtils {

    private static final Log log = LogFactory.getLog(MatchKeyUtils.class);

    private static final Set<String> domainFields = new HashSet<>(Arrays.asList("domain", "website", "url", "email"));
    private static final String latticeAccountId = "latticeaccountid";

    /**
     * This method tries to automatically resolve match keys from a list of
     * field names. It could generate incorrect result. Use with caution. It is
     * safter to directly sepcify key field mapping.
     * 
     * @param fields
     * @return
     */
    public static Map<MatchKey, String> resolveKeyMap(List<String> fields) {
        Map<MatchKey, String> keyMap = new HashMap<>();
        for (String field : fields) {
            String lowerField = field.toLowerCase();
            if (domainFields.contains(lowerField)) {
                if (!keyMap.containsKey(MatchKey.Domain) || lowerField.contains("domain")) {
                    keyMap.put(MatchKey.Domain, field);
                }
            } else if (lowerField.contains("name")) {
                keyMap.put(MatchKey.Name, field);
            } else if (lowerField.contains("city")) {
                keyMap.put(MatchKey.City, field);
            } else if (lowerField.contains("state") || lowerField.contains("province")) {
                keyMap.put(MatchKey.State, field);
            } else if (lowerField.contains("country")) {
                keyMap.put(MatchKey.Country, field);
            } else if (lowerField.contains("duns")) {
                keyMap.put(MatchKey.DUNS, field);
            } else if (latticeAccountId.equals(lowerField)) {
                keyMap.put(MatchKey.LatticeAccountID, field);
            }
        }

        log.info("Resolved KeyMap from fields " + fields + " : " + JsonUtils.serialize(keyMap));

        return keyMap;
    }

}
