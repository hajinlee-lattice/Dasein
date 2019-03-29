package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import avro.shaded.com.google.common.base.Preconditions;

public final class MatchUtils {

    private static final Logger log = LoggerFactory.getLogger(MatchUtils.class);

    static String getLegacyMatchConfigForAccount(MatchInput baseMatchInput, Set<String> columnNames) {
        MatchTransformerConfig config = new MatchTransformerConfig();
        baseMatchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        baseMatchInput.setOperationalMode(OperationalMode.LDC_MATCH);
        baseMatchInput.setKeyMap(getAccountMatchKeys(columnNames));
        baseMatchInput.setPartialMatchEnabled(true);
        config.setMatchInput(baseMatchInput);
        return JsonUtils.serialize(config);
    }

    static String getAllocateIdMatchConfigForAccount(MatchInput baseMatchInput, Set<String> columnNames) {
        MatchTransformerConfig config = new MatchTransformerConfig();
        baseMatchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);

        baseMatchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
        baseMatchInput.setTargetEntity(BusinessEntity.Account.name());
        baseMatchInput.setAllocateId(true);
        baseMatchInput.setFetchOnly(false);

        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        entityKeyMap.setKeyMap(getAccountMatchKeys(columnNames));
        if (entityKeyMap.getKeyMap().containsKey(MatchKey.SystemId)) {
            // This should not happen because nothing is setting SystemId.
            log.error("SystemId somehow set in KeyMap before MergeAccount!");
        } else {
            // TODO(jwinter): Support other SystemIds in M28.
            // For now, we hard code the SystemID MatchKey and SystemId Priority List to contain only AccountId.
            List<String> systemIdList = Collections.singletonList(InterfaceName.AccountId.toString());
            entityKeyMap.getKeyMap().put(MatchKey.SystemId, systemIdList);
        }

        Map<String, MatchInput.EntityKeyMap> entityKeyMaps = new HashMap<>();
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        baseMatchInput.setEntityKeyMaps(entityKeyMaps);
        config.setMatchInput(baseMatchInput);
        return JsonUtils.serialize(config);
    }

    static String getFetchOnlyMatchConfigForAccount(MatchInput baseMatchInput) {
        MatchTransformerConfig config = new MatchTransformerConfig();
        baseMatchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
        baseMatchInput.setTargetEntity(BusinessEntity.Account.name());

        // Fetch Only Match specific settings.
        baseMatchInput.setPredefinedSelection(ColumnSelection.Predefined.Seed);
        baseMatchInput.setAllocateId(false);
        baseMatchInput.setFetchOnly(true);

        // Prepare Entity Key Map for Fetch Only Match.
        Map<MatchKey, List<String>> keyMap = //
                MatchKeyUtils.resolveKeyMap(Collections.singletonList(InterfaceName.EntityId.name()));
        keyMap.put(MatchKey.EntityId, Collections.singletonList(InterfaceName.EntityId.name()));
        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        entityKeyMap.setKeyMap(keyMap);
        Map<String, MatchInput.EntityKeyMap> entityKeyMaps = new HashMap<>();
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        baseMatchInput.setEntityKeyMaps(entityKeyMaps);
        config.setMatchInput(baseMatchInput);
        return JsonUtils.serialize(config);
    }

    static String getAllocateIdMatchConfigForContact(MatchInput baseMatchInput, Set<String> columnNames) {
        MatchTransformerConfig config = new MatchTransformerConfig();

        baseMatchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
        baseMatchInput.setTargetEntity(BusinessEntity.Account.name());
        // lookup mode for lead to account match
        baseMatchInput.setAllocateId(false);
        baseMatchInput.setFetchOnly(false);
        baseMatchInput.setPredefinedSelection(ColumnSelection.Predefined.LeadToAcct);
        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        entityKeyMap.setKeyMap(getContactMatchKeys(columnNames));
        baseMatchInput.setEntityKeyMaps(Collections.singletonMap(BusinessEntity.Account.name(), entityKeyMap));

        config.setMatchInput(baseMatchInput);
        return JsonUtils.serialize(config);
    }

    private static Map<MatchKey, List<String>> getAccountMatchKeys(Set<String> columnNames) {
        Map<MatchKey, List<String>> matchKeys = new HashMap<>();
        addLDCMatchKeysIfExist(columnNames, matchKeys);
        log.info("Using match keys: " + JsonUtils.serialize(matchKeys));
        return matchKeys;
    }

    private static Map<MatchKey, List<String>> getContactMatchKeys(Set<String> columnNames) {
        // email is a required for lead to account match
        Preconditions.checkArgument(columnNames.contains(InterfaceName.Email.name()),
                "Missing mandatory column (Email) in input table for lead to account match");
        Map<MatchKey, List<String>> matchKeys = new HashMap<>();
        // for domain match key, value in Email column is considered first, if no value
        // for Email, try to use value in Website column instead (set in
        // addLDCMatchKeysIfExist)
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Domain, InterfaceName.Email.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.SystemId, InterfaceName.CustomerAccountId.name());
        addLDCMatchKeysIfExist(columnNames, matchKeys);
        log.info("Lead to account match keys = {}", JsonUtils.serialize(matchKeys));
        return matchKeys;
    }

    /**
     * Add all LDC match keys to the key map only if they are provided in the import
     * file.
     *
     * @param cols
     *            columns in the import file
     * @param keyMap
     *            key map that will be used for bulk match
     */
    private static void addLDCMatchKeysIfExist(Set<String> cols, Map<MatchKey, List<String>> keyMap) {
        addMatchKeyIfExists(cols, keyMap, MatchKey.Domain, InterfaceName.Website.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.DUNS, InterfaceName.DUNS.name());

        addMatchKeyIfExists(cols, keyMap, MatchKey.Name, InterfaceName.CompanyName.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.City, InterfaceName.City.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.State, InterfaceName.State.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.Country, InterfaceName.Country.name());

        addMatchKeyIfExists(cols, keyMap, MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        addMatchKeyIfExists(cols, keyMap, MatchKey.Zipcode, InterfaceName.PostalCode.name());
    }

    /*
     * if columnName exists in cols (columns of import file), add columnName to the
     * list in the keyMap (for the specified match key). a new list will be created
     * if not exist.
     */
    private static void addMatchKeyIfExists(Set<String> cols, Map<MatchKey, List<String>> keyMap, MatchKey key, String columnName) {
        if (cols.contains(columnName)) {
            keyMap.putIfAbsent(key, new ArrayList<>());
            keyMap.get(key).add(columnName);
        }
    }

}
