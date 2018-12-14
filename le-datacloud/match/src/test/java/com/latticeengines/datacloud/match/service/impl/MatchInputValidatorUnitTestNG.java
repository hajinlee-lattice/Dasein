package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;

public class MatchInputValidatorUnitTestNG {

    private final int maxRealTimeInput = 1000;

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
        Boolean failed;


        // Test 1:  Fail on missing fields.
        failed = false;

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing fields.");


        // Test 2:  Fail on empty Match Key map.
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


        // Test 3:  Fail on missing Match Key value element not found in input fields.
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


        // Test 4:  Fail on empty input data.
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


        // Test 5:  Fail on too many data rows.
        failed = false;
        input.setData(generateMockData(2000));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on too many data.");


        // Test 6:  Pass on valid data.
        failed = false;
        input.setData(generateMockData(100));
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
            System.out.println("Match Input Validation failed unexpectedly with exception: " +  e.getMessage());
            e.printStackTrace();
        }
        Assert.assertFalse(failed, "Should pass on valid data.");


        // Test 7:  Pass on DUNS match only.
        keyMap.clear();
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));
        input.setKeyMap(keyMap);
        input.setData(generateMockData(100));
        input.setSkipKeyResolution(true);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
            System.out.println("Match Input Validation failed unexpectedly with exception: " +  e.getMessage());
            e.printStackTrace();
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

        // Test 1:  Input fields not set.
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


        // Test 2:  Real time Entity Match but allocate ID set to true.
        failed = false;
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country", "DUNS",
                "SfdcId", "MktoId"));
        input.setAllocateId(true);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (UnsupportedOperationException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("Real Time Entity Match only supports non-Allocate mode."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Should fail when in Allocate ID mode for Real Time Entity Match.");


        // Test 3:  Predefined Selection not set.
        failed = false;
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


        // Test 4:  Custom Selection is set.
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


        // Test 5:  Only valid predefined column selections are allowed.
        failed = false;
        input.setCustomSelection(null);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (UnsupportedOperationException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains(
                    "Only Predefined selection [Enrichment, DerivedColumns, Model, Segment, ID, RTS, TalkingPoint, "
                    + "CompanyProfile] are supported at this time."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Predefined column selection Predefined.LeadEnrichment should not be supported.");


        // Test 6:  Entity Key Map must be populated.
        failed = false;
        input.setPredefinedSelection(Predefined.Segment);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains("MatchInput for Entity Match must contain EntityKeyMap"));
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Should fail when Entity Key Map is empty or null.");


        // Test 7:  Key Map cannot be null or empty.
        failed = false;
        input.setEntityKeyMapList(new ArrayList<>());
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        entityKeyMap.setBusinessEntity("Account");
        entityKeyMap.setSystemIdPriority(Arrays.asList("ID"));
        input.getEntityKeyMapList().add(entityKeyMap);

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains(
                    "Have to provide a key map, when skipping automatic key resolution."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "Should fail when Key Map inside Entity Key Map is empty.");


        // Test 8:  Match Key key cannot be null.
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
        input.getEntityKeyMapList().get(0).setKeyMap(keyMap);

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


        // Test 9: Match Keys should all be contained in input fields.
        failed = false;
        input.getEntityKeyMapList().get(0).getKeyMap().remove(null);
        input.getEntityKeyMapList().get(0).getKeyMap().put(MatchKey.State,
                Collections.singletonList("????"));

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


        // Test 10:  Match Key value list cannot contain null or empty elements.
        failed = false;
        input.getEntityKeyMapList().get(0).getKeyMap().put(MatchKey.State,
                Collections.singletonList("State_Province"));
        input.getEntityKeyMapList().get(0).getKeyMap().put(MatchKey.SystemId, Arrays.asList("ID", null, "MktoId"));

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


        // Test 11:  Should fail on System ID MatchKey values / System ID priority list length mismatch.
        failed = false;
        input.getEntityKeyMapList().get(0).getKeyMap().put(MatchKey.SystemId, Arrays.asList("ID", "SfdcId", "MktoId"));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains(
                    "System ID MatchKey values and System ID priority list are not the same size."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "System ID MatchKey values and System ID priority list must be same length.");


        // Test 12:  Should fail on System Id priority mismatch.
        failed = false;
        entityKeyMap.setSystemIdPriority(Arrays.asList("ID", "MktoId", "SfdcId"));

        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (IllegalArgumentException e) {
            failed = true;
            Assert.assertTrue(e.getMessage().contains(
                    "System ID MatchKey values and System ID priority list mismatch at index 1."),
                    "Wrong error message: " + e.getMessage());
        } catch (Exception e) {
            Assert.fail("Failed on wrong exception: " + e.getMessage());
        }
        Assert.assertTrue(failed, "System ID MatchKey values and System ID priority list must match.");


        // Test 13:  Should fail on empty data.
        failed = false;
        entityKeyMap.setSystemIdPriority(Arrays.asList("ID", "SfdcId", "MktoId"));

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


        // Test 14:  Should fail on wrong size input data.
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


        // Test 15:  Should pass on valid data.
        failed = false;
        input.setData(generateMockData(100, true));

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
