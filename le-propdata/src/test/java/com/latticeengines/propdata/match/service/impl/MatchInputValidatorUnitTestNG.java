package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
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
        input.setTenant(new Tenant("PD_Test"));

        failed = false;
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing selection.");
    }

    @Test(groups = "unit")
    public void testRealTimeValidation() {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant("PD_Test"));
        input.setPredefinedSelection(ColumnSelection.Predefined.Model);

        Boolean failed = false;
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing fields.");
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country"));

        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        input.setKeyMap(keyMap);

        failed = false;
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on emtpy key map.");
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.City, Collections.singletonList("City"));
        keyMap.put(MatchKey.State, Collections.singletonList("????"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));

        failed = false;
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on missing target field.");

        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.City, Collections.singletonList("City"));
        keyMap.put(MatchKey.State, Collections.singletonList("State_Province"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));

        failed = false;
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on empty data.");

        failed = false;
        input.setData(generateMockData(2000));
        try {
            MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
        } catch (Exception e) {
            failed = true;
        }
        Assert.assertTrue(failed, "Should failed on too many data.");

        input.setData(generateMockData(100));
        MatchInputValidator.validateRealTimeInput(input, maxRealTimeInput);
    }

    static List<List<Object>> generateMockData(int rows) {
        List<List<Object>> data = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            String domain = "abc@" + randomString(6) + ".com";
            String name = randomString(20);
            String city = randomString(20);
            String state = randomString(10);
            String country = "USA";
            List<Object> row = Arrays.asList((Object) i, domain, name, city, state, country);
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

}
