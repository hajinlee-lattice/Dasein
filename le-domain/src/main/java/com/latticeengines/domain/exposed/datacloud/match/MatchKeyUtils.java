package com.latticeengines.domain.exposed.datacloud.match;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.kitesdk.shaded.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class MatchKeyUtils {

    /**
     * Map MatchKey to field names in AccountMaster
     */
    public static final Map<MatchKey, String> AM_FIELD_MAP = ImmutableMap
            .<MatchKey, String> builder() //
            .put(MatchKey.LatticeAccountID, DataCloudConstants.LATTICE_ID) //
            .put(MatchKey.DUNS, DataCloudConstants.ATTR_LDC_DUNS) //
            .put(MatchKey.Domain, DataCloudConstants.ATTR_LDC_DOMAIN) //
            .put(MatchKey.Name, DataCloudConstants.ATTR_LDC_NAME) //
            .put(MatchKey.Country, DataCloudConstants.ATTR_COUNTRY) //
            .put(MatchKey.State, DataCloudConstants.ATTR_STATE) //
            .put(MatchKey.City, DataCloudConstants.ATTR_CITY) //
            .build();
    /**
     * Map MatchKey to field names in AccountMasterSeed (before adding LDC_
     * prefix)
     */
    public static final Map<MatchKey, String> AMS_FIELD_MAP = ImmutableMap
            .<MatchKey, String> builder() //
            .put(MatchKey.LatticeAccountID, DataCloudConstants.LATTICE_ID) //
            .put(MatchKey.DUNS, DataCloudConstants.AMS_ATTR_DUNS) //
            .put(MatchKey.Domain, DataCloudConstants.AMS_ATTR_DOMAIN) //
            .put(MatchKey.Name, DataCloudConstants.AMS_ATTR_NAME) //
            .put(MatchKey.Country, DataCloudConstants.AMS_ATTR_COUNTRY) //
            .put(MatchKey.State, DataCloudConstants.AMS_ATTR_STATE) //
            .put(MatchKey.City, DataCloudConstants.AMS_ATTR_CITY) //
            .build();
    private static final Logger log = LoggerFactory.getLogger(MatchKeyUtils.class);
    private static final List<String> domainFields = new ArrayList<>(
            Arrays.asList("domain", "website", "email", "url"));
    private static final String latticeAccountId = "latticeaccountid";
    /**
     * Match key level -> accuracy level Lower accuracy level, less information
     * in match input
     */
    private static final Map<MatchKey, Integer> KEY_LEVEL = ImmutableMap.of( //
            MatchKey.Name, 0, //
            MatchKey.Country, 1, //
            MatchKey.State, 2, //
            MatchKey.City, 3 //
    );

    /**
     * Don't allow reserved fields appear in match input fields (Non-FetchOnly
     * mode)
     */
    private static final Set<String> ENTITY_RESERVED_FIELDS = ImmutableSet.of( //
            InterfaceName.EntityId.name(), //
            InterfaceName.AccountId.name(), //
            InterfaceName.ContactId.name() //
            );

    public static Map<MatchKey, List<String>> resolveKeyMap(Schema schema) {
        List<String> fieldNames = new ArrayList<>();
        for (Schema.Field field : schema.getFields()) {
            fieldNames.add(field.name());
        }
        return resolveKeyMap(fieldNames);
    }

    /**
     * This method tries to automatically resolve match keys from a list of
     * field names. It could generate incorrect result. Use with caution. It is
     * safer to directly specify key field mapping.
     *
     * @param fields
     * @return
     */
    public static Map<MatchKey, List<String>> resolveKeyMap(List<String> fields) {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();

        for (String domainField : domainFields) {
            for (String field : fields) {
                String lowerField = field.toLowerCase();
                if (domainField.equals(lowerField)) {
                    if (keyMap.get(MatchKey.Domain) == null) {
                        keyMap.put(MatchKey.Domain, new ArrayList<>());
                    }
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
            case "entityid":
                keyMap.put(MatchKey.EntityId, Collections.singletonList(field));
                    break;
            default:
            }
        }

        log.debug("Resolved KeyMap from fields " + fields + " : " + JsonUtils.serialize(keyMap));

        return keyMap;
    }

    /**
     * Only to evaluate name/location based match key level Match key tuple
     * without name should not be accepted by name/location based match TODO:
     * Evaluate key level for phone number and zipcode
     */
    public static MatchKey evalKeyLevel(MatchKeyTuple tuple) {
        if (StringUtils.isNotBlank(tuple.getName()) //
                && StringUtils.isNotBlank(tuple.getCountryCode()) //
                && StringUtils.isNotBlank(tuple.getCity())) {
            return MatchKey.City;
        }
        if (StringUtils.isNotBlank(tuple.getName()) //
                && StringUtils.isNotBlank(tuple.getCountryCode()) //
                && StringUtils.isNotBlank(tuple.getState())) {
            return MatchKey.State;
        }
        if (StringUtils.isNotBlank(tuple.getName()) //
                && StringUtils.isNotBlank(tuple.getCountryCode())) {
            return MatchKey.Country;
        }
        if (StringUtils.isNotBlank(tuple.getName())) {
            return MatchKey.Name;
        }
        return null;
    }

    /**
     * Compare accuracy of match key level Lower accuracy level, less
     * information in match input Return 0: same key level Return -1: compared
     * has lower accuracy than compareTo Return 1: compared has higher accuracy
     * than compareTo
     */
    public static int compareKeyLevel(MatchKey compared, MatchKey compareTo) {
        if (!KEY_LEVEL.containsKey(compared) || !KEY_LEVEL.containsKey(compareTo)) {
            throw new UnsupportedOperationException(String.format(
                    "Not able to compare match key level between %s and %s", compared, compareTo));
        }
        if (KEY_LEVEL.get(compared) > KEY_LEVEL.get(compareTo)) {
            return 1;
        }
        if (KEY_LEVEL.get(compared) < KEY_LEVEL.get(compareTo)) {
            return -1;
        }
        return 0;
    }

    /**
     * Evaluate key partition based on the input {@link MatchKeyTuple}.
     * Currently, only name/location based match key partition is used.
     *
     * @param tuple
     *            input match keys
     * @return {@literal null} if no name/location fields present. otherwise, a
     *         string representing the partition is returned.
     */
    public static String evalKeyPartition(MatchKeyTuple tuple) {
        if (tuple == null) {
            return null;
        }

        List<String> keys = new ArrayList<>();
        // relevant fields
        if (StringUtils.isNotBlank(tuple.getName())) {
            keys.add(MatchKey.Name.name());
        }
        if (StringUtils.isNotBlank(tuple.getCountryCode())) {
            keys.add(MatchKey.Country.name());
        }
        if (StringUtils.isNotBlank(tuple.getCity())) {
            keys.add(MatchKey.City.name());
        }
        if (StringUtils.isNotBlank(tuple.getState())) {
            keys.add(MatchKey.State.name());
        }

        return buildKeyPartition(keys);
    }

    /**
     * Sort and return comma separated key names
     *
     * @param keys
     * @return
     */
    public static String buildKeyPartition(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return null;
        }
        List<String> sortedKeys = keys.stream().sorted().collect(Collectors.toList());
        return String.join(",", sortedKeys);
    }

    // Create a Map from field name to its column position.
    private static Map<String, Integer> getFieldPositions(MatchInput input) {
        return IntStream.range(0, input.getFields().size()).boxed()
                .collect(Collectors.toMap(pos -> input.getFields().get(pos), pos -> pos));
    }

    /**
     * Field name is treated as case sensitive, because field name in avro data
     * in Dynamo is case sensitive
     *
     * @param input
     * @return MatchKey -> list of match key's corresponding field indexes in
     *         MatchInput.data array
     */
    public static Map<MatchKey, List<Integer>> getKeyPositionMap(MatchInput input) {
        Map<String, Integer> fieldPos = getFieldPositions(input);

        Map<MatchKey, List<Integer>> posMap = input.getKeyMap().keySet().stream()
                .collect(Collectors.toMap(key -> key,
                        key -> input.getKeyMap().get(key).stream().map(field -> fieldPos.get(field))
                                .collect(Collectors.toList())));

        return posMap;
    }

    /**
     * Field name is treated as case sensitive, because field name in avro data
     * in Dynamo is case sensitive
     *
     * @param input
     * @return Entity -> (MatchKey -> list of match key's corresponding field
     *         indexes in MatchInput.data array)
     */
    public static Map<String, Map<MatchKey, List<Integer>>> getEntityKeyPositionMaps(MatchInput input) {
        Map<String, Integer> fieldPos = getFieldPositions(input);

        Map<String, Map<MatchKey, List<Integer>>> posMap = input.getEntityKeyMaps().entrySet().stream()
                .collect(Collectors.toMap( //
                        Map.Entry::getKey, //
                        entityKeyMapEntry -> entityKeyMapEntry.getValue().getKeyMap().entrySet().stream()
                                .collect(Collectors.toMap( //
                                        Map.Entry::getKey, //
                                        keyMapEntry -> keyMapEntry.getValue().stream()
                                                .map(field -> fieldPos.get(field)).collect(Collectors.toList())))));

        return posMap;

    }

    public static MatchKeyTuple createAccountMatchKeyTuple(EntityMatchKeyRecord entityRecord) {
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        NameLocation nameLocationInfo = entityRecord.getParsedNameLocation();
        if (nameLocationInfo != null) {
            matchKeyTuple.setCity(nameLocationInfo.getCity());
            matchKeyTuple.setCountry(nameLocationInfo.getCountry());
            matchKeyTuple.setCountryCode(nameLocationInfo.getCountryCode());
            matchKeyTuple.setName(nameLocationInfo.getName());
            matchKeyTuple.setState(nameLocationInfo.getState());
            matchKeyTuple.setZipcode(nameLocationInfo.getZipcode());
            matchKeyTuple.setPhoneNumber(nameLocationInfo.getPhoneNumber());
        }
        if (!entityRecord.isPublicDomain() || entityRecord.isMatchEvenIsPublicDomain()) {
            matchKeyTuple.setDomain(entityRecord.getParsedDomain());
        }
        matchKeyTuple.setDuns(entityRecord.getParsedDuns());
        return matchKeyTuple;
    }

    public static MatchKeyTuple createContactMatchKeyTuple(EntityMatchKeyRecord entityRecord) {
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        Contact contact = entityRecord.getParsedContact();
        if (contact != null) {
            matchKeyTuple.setEmail(contact.getEmail());
            matchKeyTuple.setName(contact.getName());
            matchKeyTuple.setPhoneNumber(contact.getPhoneNumber());
        }
        return matchKeyTuple;
    }

    public static boolean isEntityReservedField(String field) {
        return ENTITY_RESERVED_FIELDS.contains(field);
    }

    // Map from EntityLookupEntry.Type to EntityMatchType.
    public static EntityMatchType getEntityMatchType(EntityLookupEntry.Type lookupType, MatchKeyTuple tuple) {
        switch (lookupType) {
            // Account Match Cases
            case DOMAIN_COUNTRY:
                return EntityMatchType.DOMAIN_COUNTRY;
            case NAME_COUNTRY:
                return EntityMatchType.NAME_COUNTRY;
            case DUNS:
                return EntityMatchType.DUNS;

            // Contact Match Cases
            case EMAIL:
                return EntityMatchType.EMAIL;
            case ACCT_EMAIL:
                return EntityMatchType.EMAIL_ACCOUNTID;
            case NAME_PHONE:
                return EntityMatchType.NAME_PHONE;
            case ACCT_NAME_PHONE:
                return EntityMatchType.NAME_PHONE_ACCOUNTID;

            // Joint Cases
            case EXTERNAL_SYSTEM:
                if (tuple.getSystemIds().size() > 1) {
                    log.error("Should be only one system ID per MatchKeyTyple in LookupEntry");
                    break;
                } else if (tuple.getSystemIds().size() < 1) {
                    log.error("Should be one system ID per MatchKeyTyple in LookupEntry");
                    break;
                }
                if (InterfaceName.CustomerAccountId.name().equals(tuple.getSystemIds().get(0).getKey())) {
                    return EntityMatchType.ACCOUNTID;
                } else if (InterfaceName.CustomerContactId.name().equals(tuple.getSystemIds().get(0).getKey())) {
                    return EntityMatchType.CONTACTID;
                } else {
                    return EntityMatchType.SYSTEMID;
                }

            default:
                log.error("Unsupported EntityLookupEntry.Type: " + lookupType.name());
        }
        return null;
    }
}
