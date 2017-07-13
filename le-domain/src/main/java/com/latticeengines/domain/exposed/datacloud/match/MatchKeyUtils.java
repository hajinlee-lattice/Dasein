package com.latticeengines.domain.exposed.datacloud.match;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;

public class MatchKeyUtils {

    private static final Logger log = LoggerFactory.getLogger(MatchKeyUtils.class);

    private static final List<String> domainFields = new ArrayList<>(Arrays.asList("domain", "website", "email", "url"));
    private static final String latticeAccountId = "latticeaccountid";

    public static Map<MatchKey, List<String>> resolveKeyMap(Schema schema) {
        List<String> fieldNames = new ArrayList<>();
        for (Schema.Field field: schema.getFields()) {
            fieldNames.add(field.name());
        }
        return resolveKeyMap(fieldNames);
    }

    /**
     * This method tries to automatically resolve match keys from a list of
     * field names. It could generate incorrect result. Use with caution. It is
     * safer to directly specify key field mapping.
     * @param fields
     * @return
     */
    public static Map<MatchKey, List<String>> resolveKeyMap(List<String> fields) {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();

        keyMap.put(MatchKey.Domain, new ArrayList<String>());

        for (String domainField: domainFields) {
            for (String field : fields) {
                String lowerField = field.toLowerCase();
                if (domainField.equals(lowerField)) {
                    keyMap.get(MatchKey.Domain).add(field);
                }
            }
        }

        for (String field : fields) {
            String lowerField = field.toLowerCase();
            switch (lowerField) {
                case "name":
                case "company":
                case "companyname":
                case "company_name":
                    keyMap.put(MatchKey.Name, Collections.singletonList(field));
                    break;
                case "city":
                    keyMap.put(MatchKey.City, Collections.singletonList(field));
                    break;
                case "state":
                case "province":
                case "state_province":
                    keyMap.put(MatchKey.State, Collections.singletonList(field));
                    break;
                case "zip":
                case "zipcode":
                case "zip_code":
                case "postalcode":
                case "postal_code":
                    keyMap.put(MatchKey.Zipcode, Collections.singletonList(field));
                    break;
                case "phone":
                case "phonenumber":
                case "phone_number":
                    keyMap.put(MatchKey.PhoneNumber, Collections.singletonList(field));
                    break;
                case "country":
                    keyMap.put(MatchKey.Country, Collections.singletonList(field));
                    break;
                case "duns":
                case "duns_number":
                case "dunsnumber":
                    keyMap.put(MatchKey.DUNS, Collections.singletonList(field));
                    break;
                case latticeAccountId:
                case "latticeid":
                    keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(field));
                    break;
            }
        }

        log.debug("Resolved KeyMap from fields " + fields + " : " + JsonUtils.serialize(keyMap));

        return keyMap;
    }

}
