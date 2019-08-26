package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class MatchInputValidatorUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(MatchInputValidatorUnitTestNG.class);

    private final int maxRealTimeInput = 1000;

    // At least one of required match key needs to be provided for Entity match
    private static final MatchKey[] REQUIRED_ENTITY_KEYS = { //
            MatchKey.Domain, //
            MatchKey.Name, //
            MatchKey.DUNS, //
            MatchKey.SystemId };

    @Test(groups = "unit")
    public void testGeneralValidation() {
        MatchInput input = new MatchInput();
        boolean failed = false;
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing tenant.");

        // tenant without identifier
        failed = false;
        input.setTenant(new Tenant());
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            Assert.assertNotNull(e);
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing tenant identifier.");

        // valid tenant
        failed = false;
        input.setTenant(new Tenant("PD_Test"));
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing selection.");
    }

    @Test(groups = "unit")
    public void testValidateRealTimeInput() {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant("PD_Test"));
        input.setPredefinedSelection(Predefined.Model);
        input.setDataCloudVersion("2.0.17");
        Boolean failed;


        // Fail on missing fields.
        failed = false;

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing fields.");


        // Fail on empty Match Key map.
        failed = false;
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country", "DUNS"));
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        input.setKeyMap(keyMap);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on empty key map.");


        // Fail on missing Match Key value element not found in input fields.
        failed = false;
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.City, Collections.singletonList("City"));
        keyMap.put(MatchKey.State, Collections.singletonList("????"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing MatchKey value element in input fields.");


        // Fail on empty input data.
        failed = false;
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.City, Collections.singletonList("City"));
        keyMap.put(MatchKey.State, Collections.singletonList("State_Province"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on empty data.");


        // Fail on too many data rows.
        failed = false;
        input.setData(generateMockData(2000));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on too many data.");


        // Pass on valid data.
        failed = false;
        input.setData(generateMockData(100));
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
            log.error("Match Input Validation failed unexpectedly with exception", e);
        }
        Assert.assertFalse(failed, "Should pass on valid data.");


        // Pass on DUNS match only.
        keyMap.clear();
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));
        input.setKeyMap(keyMap);
        input.setData(generateMockData(100));
        input.setSkipKeyResolution(true);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
            log.error("Match Input Validation failed unexpectedly with exception", e);
        }
        Assert.assertFalse(failed, "Should pass on DUNS only validation.");
    }


    // Test real time validation of Entity Matching.
    @Test(groups = "unit")
    public void testValidateRealTimeInputEntityMatch() {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant("PD_Test"));
        input.setSkipKeyResolution(true);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        Boolean failed;

        // Input fields not set.
        failed = false;

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("Empty list of fields."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Should fail on empty input fields.");


        // Predefined Selection not set.
        failed = false;
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country", "DUNS",
                "SfdcId", "MktoId"));
        input.setData(Collections.singletonList(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province",
                "Country", "DUNS", "SfdcId", "MktoId")));
        input.setAllocateId(false);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("Entity Match must have predefined column selection set."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Should fail on missing predefined column selection.");


        // Custom Selection is set.
        failed = false;
        input.setPredefinedSelection(Predefined.LeadEnrichment);
        input.setCustomSelection(new ColumnSelection());

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("Entity Match cannot have custom column selection set."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Should fail on custom column selection being set.");


        // Only valid predefined column selections are allowed.
        failed = false;
        input.setCustomSelection(null);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (UnsupportedOperationException e) {
            failed = true;
            Assert.assertTrue(
                    e.getMessage().contains(
                            "Only Predefined selection [ID, Seed, LeadToAcct] are supported for entity match at this time."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Predefined column selection Predefined.LeadEnrichment should not be supported.");


        // EntityKeyMaps must be populated.
        input.setPredefinedSelection(Predefined.Seed);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            Assert.fail("Should allow Entity Key Map to be empty or null. Error: " + e.getMessage());
        }


        // Each EntityKeyMap must be initialized.
        input.setEntityKeyMaps(new HashMap<>());
        input.getEntityKeyMaps().put(BusinessEntity.Account.name(), null);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            Assert.fail("Should allow EntityKeyMap for entity: " + BusinessEntity.Account.name()
                    + " to be null. Error: " + e.getMessage());
        }

        // Key Map cannot be null or empty.
        input.getEntityKeyMaps().remove(BusinessEntity.Account.name());
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        input.getEntityKeyMaps().put(BusinessEntity.Account.name(), entityKeyMap);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            Assert.fail("Should allow EntityKeyMap for entity: " + BusinessEntity.Account.name()
                    + " to be empty. Error: " + e.getMessage());
        }

        // Match Key key cannot be null.
        failed = false;
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        // Empty MatchKey value should not be a problem.
        keyMap.put(MatchKey.Domain, new ArrayList<>());
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(null, Collections.singletonList("Street"));
        // null MatchKey value should not be a problem.
        keyMap.put(MatchKey.City, null);
        keyMap.put(MatchKey.State, Collections.singletonList("State_Province"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));
        keyMap.put(MatchKey.SystemId, Arrays.asList("ID", "SfdcId", "MktoId"));
        entityKeyMap.setKeyMap(keyMap);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("MatchKey key must be non-null."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "MatchKey key cannot be null.");


        // Match Keys should all be contained in input fields.
        failed = false;
        entityKeyMap.getKeyMap().remove(null);
        entityKeyMap.getKeyMap().put(MatchKey.State, Collections.singletonList("????"));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("Cannot find MatchKey value element ???? in claimed field list."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "All match keys should appear in the list of input fields.");


        // Match Key value list cannot contain null or empty elements.
        failed = false;
        entityKeyMap.getKeyMap().put(MatchKey.State, Collections.singletonList("State_Province"));
        entityKeyMap.getKeyMap().put(MatchKey.SystemId, Arrays.asList("ID", null, "MktoId"));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("MatchKey value list elements must be non-null and non-empty."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "MatchKey value list cannot contain null or empty elements.");


        // EntityKeyMaps must contain Account Key Map.
        failed = false;
        entityKeyMap.getKeyMap().put(MatchKey.SystemId, Arrays.asList("ID", "SfdcId", "MktoId"));
        input.getEntityKeyMaps().remove(BusinessEntity.Account.name());
        input.getEntityKeyMaps().put(BusinessEntity.Transaction.name(), entityKeyMap);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            Assert.fail("Entity Key Map should be able to contain entity other than Account & Contact. Error: "
                    + e.getMessage());
        }


        // Should fail on empty data.
        failed = false;
        input.getEntityKeyMaps().put(BusinessEntity.Account.name(), entityKeyMap);
        input.setData(Collections.emptyList());

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("Empty input data."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Input data must be non-empty.");


        // Should fail on wrong size input data.
        failed = false;
        input.setData(generateMockData(100, true));
        // Add extra row to input data with two many elements.
        input.getData().add(Arrays.asList((Object) 100, "1", "2", "3", "4", "5", "6", "7", "8", "9"));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains(
                    "Input data length must be less than or equal to input fields length."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Input data must not be longer than number of input fields.");

        // Should fail on unmatched decision graph and target entity.
        failed = false;
        input.setData(generateMockData(100, true));
        // Fake some decision graph name just for testing purpose
        input.setDecisionGraph("AccountDecisionGraph");
        input.setTargetEntity(BusinessEntity.Contact.name());
        DecisionGraph decisionGraph = new DecisionGraph();
        decisionGraph.setEntity(BusinessEntity.Account.name());
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput, decisionGraph);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains(
                    "Decision graph AccountDecisionGraph and target entity Contact are not matched. Target entity for decision graph AccountDecisionGraph is Account"),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed,
                "Decision graph AccountDecisionGraph and target entity Contact are not matched. Target entity for decision graph AccountDecisionGraph is Account");


        // Should pass on valid data.
        failed = false;
        input.setData(generateMockData(100, true));
        input.setDecisionGraph(null);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
            System.out.println("Match Input Validation failed unexpectedly with exception: " +  e.getMessage());
            e.printStackTrace();
        }
        Assert.assertFalse(failed, "Match Input Validation should have passed.");
    }

    // TODO(jwinter):  When Entity Match Bulk Match is completed, move these tests into Bulk Match Validation unit
    //     testing and make validateEntityMatchColumnSelection() function in
    //     le-datacloud/match/src/main/java/com/latticeengines/datacloud/match/service/impl/MatchInputValidator
    //     private again.
    // Cover some test cases in Entity Match Column Selection not testable using Real Time Match because they rely
    // on allocate ID being set.
    @Test(groups = "unit")
    public void testValidateEntityMatchColumnSelection() {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant("PD_Test"));
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        Boolean failed;

        // Test 1:  Allocated ID mode but predefined column selection not set to ID.
        failed = false;
        input.setAllocateId(true);
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country", "DUNS"));
        input.setPredefinedSelection(Predefined.Model);

        try {
            MatchInputValidator.validateEntityMatchColumnSelection(input);
        } catch (UnsupportedOperationException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains(
                    "Entity Match Allocate ID mode only supports predefined column selection set to \"ID\""));
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Should fail predefined column selection not set to ID for allocate ID mode.");


        // Test 2:  Allocated ID mode should pass validation when predefined column selection is set to ID.
        failed = false;
        input.setPredefinedSelection(Predefined.ID);
        try {
            MatchInputValidator.validateEntityMatchColumnSelection(input);
        } catch (Exception e) {
            failed = true;
            System.out.println("Entity Match Column Selection Validation failed unexpectedly with exception: "
                    +  e.getMessage());
            e.printStackTrace();
        }
        Assert.assertFalse(failed, "Entity Match Column Selection Validation should have passed.");
    }

    /*
     * use of MatchKey.PreferredEntityId in non-allocate ID mode is not allowed
     */
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Not allowed to have preferred entity id in non-allocate mode.*")
    private void validatePreferredIdMatchKey() {
        MatchInput input = new MatchInput();
        // non-allocate mode
        input.setAllocateId(false);
        // other required fields, nothing important
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTenant(new Tenant("test_tenant_id"));
        input.setFields(Arrays.asList(MatchKey.PreferredEntityId.name(), InterfaceName.CustomerContactId.name()));
        input.setData(Collections.singletonList(Arrays.asList("account_id_123", "contact_cid_123")));
        input.setPredefinedSelection(Predefined.ID);

        // account key map contains preferred entity id
        EntityKeyMap accountMap = new EntityKeyMap();
        accountMap.addMatchKey(MatchKey.PreferredEntityId, MatchKey.PreferredEntityId.name());
        // contact key map is valid since it has only system ID
        EntityKeyMap contactMap = new EntityKeyMap();
        contactMap.addMatchKey(MatchKey.SystemId, "CustomerContactId");
        Map<String, EntityKeyMap> entityKeyMaps = new HashMap<>();
        entityKeyMaps.put(BusinessEntity.Account.name(), accountMap);
        input.setEntityKeyMaps(entityKeyMaps);

        MatchInputValidator.validateRealTimeInput(input, 100);
    }

    @Test(groups = "unit", dataProvider = "requiredAccountMatchKey")
    public void testValidateAccountMatchKeysNonFetchOnly3(MatchKey[] keys) {
        // Set required match key and map field. Should pass without exception
        validateAccountMatchKey(keys, true, false);
    }

    @Test(groups = "unit", dataProvider = "requiredAccountMatchKey", //
            expectedExceptions = { IllegalArgumentException.class }, //
            expectedExceptionsMessageRegExp = "For fetch-only mode Account match, must provide EntityId match key")
    public void testValidateAccountMatchKeysFetchOnly1(MatchKey[] keys) {
        // Don't provide EntityId for fetch-only mode
        validateAccountMatchKey(keys, true, true);
    }

    @Test(groups = "unit", dataProvider = "requiredAccountMatchKeyFetchOnly")
    public void testValidateAccountMatchKeysFetchOnly2(MatchKey[] keys) {
        // Fetch-only mode: Set required match key and map field. Should pass
        // without exception
        validateAccountMatchKey(keys, true, true);
    }

    private void validateAccountMatchKey(MatchKey[] keys, boolean mapField, boolean fetchOnly) {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant("PD_Test"));
        input.setSkipKeyResolution(true);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setPredefinedSelection(Predefined.Seed);

        input.setFetchOnly(fetchOnly);
        input.setEntityKeyMaps(new HashMap<>());
        input.getEntityKeyMaps().put(BusinessEntity.Account.name(), new EntityKeyMap());
        EntityKeyMap entityKeyMap = input.getEntityKeyMaps().get(BusinessEntity.Account.name());
        entityKeyMap.setKeyMap(new HashMap<>());
        Map<MatchKey, List<String>> keyMap = entityKeyMap.getKeyMap();
        if (keys != null) {
            for (MatchKey key : keys) {
                keyMap.put(key, new ArrayList<>());
                if (mapField) {
                    keyMap.get(key).add(key.name());
                }
            }
        }

        // This test doesn't care what fields and data are set
        List<String> fields = Arrays.stream(MatchKey.values()).map(key -> key.name()).collect(Collectors.toList());
        if (!fetchOnly) {
            fields = fields.stream().filter(field -> !MatchKeyUtils.isEntityReservedField(field))
                    .collect(Collectors.toList());
        }
        input.setFields(fields);
        input.setData(generateMockData(100, true));

        MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
    }

    @DataProvider(name = "requiredAccountMatchKey")
    public Object[][] dataRequiredAccountMatchKey() {
        return getAllMatchKeyCombinations(REQUIRED_ENTITY_KEYS);
    }

    @DataProvider(name = "requiredAccountMatchKeyFetchOnly")
    public Object[][] dataRequiredAccountMatchKeyFetchOnly() {
        return new Object[][] { //
                { new MatchKey[] { MatchKey.EntityId } }, //
        };
    }

    // Return all combinations of match keys as test data provider
    private Object[][] getAllMatchKeyCombinations(MatchKey... keys) {
        int total = (int) Math.pow(2d, Double.valueOf(keys.length));
        Object[][] combinations = new Object[total][];
        for (int i = 1; i < total; i++) {
            String code = Integer.toBinaryString(total | i).substring(1);
            List<MatchKey> combination = new ArrayList<>();
            for (int j = 0; j < keys.length; j++) {
                if (code.charAt(j) == '1') {
                    combination.add(keys[j]);
                }
            }
            combinations[i - 1] = combination.toArray();
        }
        return combinations;
    }

    static List<List<Object>> generateMockData(int rows) {
        return generateMockData(rows, false);
    }

    static List<List<Object>> generateMockData(int rows, boolean withSystemId) {
        List<List<Object>> data = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            String domain = "abc@" + randomString(6) + ".com";
            String name = randomString(20);
            String city = randomString(20);
            String state = randomString(10);
            String country = "USA";
            String duns = randomString(10);
            String sfdcid, mktoid;
            List<Object> row;
            if (withSystemId) {
                sfdcid = randomId(12);
                mktoid = randomId(15);
                row = Arrays.asList((Object) i, domain, name, city, state, country, duns, sfdcid, mktoid);
            } else {
                row = Arrays.asList((Object) i, domain, name, city, state, country, duns);
            }
            data.add(row);
        }
        return data;
    }

    private static String randomString(int length) {
        Random random = new Random();
        String characters = "abcdefghijklmnopqrstuvwxyz0123456789";
        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(random.nextInt(characters.length()));
        }
        return new String(text);
    }

    private static String randomId(int length) {
        Random random = new Random();
        String characters = "0123456789";
        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(random.nextInt(characters.length()));
        }
        return new String(text);
    }


}
