package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public final class MatchUtils {

    private static final Logger log = LoggerFactory.getLogger(MatchUtils.class);

    static String getLegacyMatchConfigForAccount(String customer, MatchInput baseMatchInput, Set<String> columnNames) {
        MatchTransformerConfig config = new MatchTransformerConfig();
        baseMatchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        baseMatchInput.setOperationalMode(OperationalMode.LDC_MATCH);
        baseMatchInput.setKeyMap(getAccountMatchKeysAccount(columnNames));
        baseMatchInput.setPartialMatchEnabled(true);
        baseMatchInput.setTenant(new Tenant(CustomerSpace.parse(customer).toString()));
        config.setMatchInput(baseMatchInput);
        return JsonUtils.serialize(config);
    }

    static String getAllocateIdMatchConfigForAccount(String customer, MatchInput baseMatchInput,
                                                     Set<String> columnNames) {
        MatchTransformerConfig config = new MatchTransformerConfig();
        baseMatchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
        baseMatchInput.setTargetEntity(BusinessEntity.Account.name());
        baseMatchInput.setAllocateId(true);
        baseMatchInput.setOutputNewEntities(false);
        baseMatchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        baseMatchInput.setTenant(new Tenant(CustomerSpace.parse(customer).toString()));
        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        entityKeyMap.setKeyMap(getAccountMatchKeysAccount(columnNames));
        Map<String, MatchInput.EntityKeyMap> entityKeyMaps = new HashMap<>();
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        baseMatchInput.setEntityKeyMaps(entityKeyMaps);

        config.setMatchInput(baseMatchInput);
        return JsonUtils.serialize(config);
    }

    static String getAllocateIdMatchConfigForContact(String customer, MatchInput baseMatchInput,
                                                     Set<String> columnNames, String newAccountTableName) {
        MatchTransformerConfig config = new MatchTransformerConfig();
        baseMatchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
        baseMatchInput.setTargetEntity(BusinessEntity.Contact.name());
        baseMatchInput.setAllocateId(true);
        baseMatchInput.setOutputNewEntities(true);
        baseMatchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        baseMatchInput.setTenant(new Tenant(CustomerSpace.parse(customer).toString()));
        MatchInput.EntityKeyMap accountKeyMap = MatchInput.EntityKeyMap.fromKeyMap(getAccountMatchKeysForContact(columnNames));
        MatchInput.EntityKeyMap contactKeyMap = MatchInput.EntityKeyMap.fromKeyMap(getContactMatchKeys(columnNames));
        baseMatchInput.setEntityKeyMaps(new HashMap<>(ImmutableMap.of( //
                BusinessEntity.Account.name(), accountKeyMap, //
                BusinessEntity.Contact.name(), contactKeyMap
        )));

        config.setMatchInput(baseMatchInput);
        config.setNewEntitiesTableName(newAccountTableName);
        return JsonUtils.serialize(config);
    }

    private static Map<MatchKey, List<String>> getAccountMatchKeysAccount(Set<String> columnNames) {
        return getAccountMatchKeys(columnNames, false);
    }

    private static Map<MatchKey, List<String>> getAccountMatchKeysForContact(Set<String> columnNames) {
        return getAccountMatchKeys(columnNames, true);
    }

    private static Map<MatchKey, List<String>> getAccountMatchKeys(Set<String> columnNames, boolean considerEmail) {
        Map<MatchKey, List<String>> matchKeys = new HashMap<>();
        if (considerEmail) {
            addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Domain, InterfaceName.Email.name());
        }
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Domain, InterfaceName.Website.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.DUNS, InterfaceName.DUNS.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Name, InterfaceName.CompanyName.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.City, InterfaceName.City.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.State, InterfaceName.State.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Country, InterfaceName.Country.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Zipcode, InterfaceName.PostalCode.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.SystemId, InterfaceName.CustomerAccountId.name());
        log.info("Account match keys = {}", JsonUtils.serialize(matchKeys));
        return matchKeys;
    }

    private static Map<MatchKey, List<String>> getContactMatchKeys(Set<String> columnNames) {
        Map<MatchKey, List<String>> matchKeys = new HashMap<>();
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Name, InterfaceName.ContactName.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Country, InterfaceName.Country.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.Email, InterfaceName.Email.name());
        addMatchKeyIfExists(columnNames, matchKeys, MatchKey.SystemId, InterfaceName.CustomerContactId.name());
        log.info("Contact match keys = {}", JsonUtils.serialize(matchKeys));
        return matchKeys;
    }

    /*
     * if columnName exists in cols (columns of import file), add columnName to the
     * list in the keyMap (for the specified match key). a new list will be created
     * if not exist.
     */
    private static void addMatchKeyIfExists(Set<String> cols, Map<MatchKey, List<String>> keyMap,
                                            MatchKey key, String columnName) {
        if (cols.contains(columnName)) {
            keyMap.putIfAbsent(key, new ArrayList<>());
            keyMap.get(key).add(columnName);
        }
    }

}
