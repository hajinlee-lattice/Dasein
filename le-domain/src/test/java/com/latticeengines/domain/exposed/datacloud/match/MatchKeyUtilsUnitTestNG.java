package com.latticeengines.domain.exposed.datacloud.match;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class MatchKeyUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "resolveDomainDataProvider")
    public void testResolveDomain(String[] fields, String[] expectedFields) throws IOException {
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(Arrays.asList(fields));
        List<String> resolvedFields = keyMap.get(MatchKey.Domain);
        Assert.assertEquals(resolvedFields.size(), expectedFields.length);
        if (expectedFields.length > 0) {
            for (int i = 0; i < resolvedFields.size(); i++) {
                Assert.assertEquals(resolvedFields.get(i), expectedFields[i],
                        String.format("Expected [%s] to be [%s]",
                                StringUtils.join(resolvedFields, ", "),
                                StringUtils.join(expectedFields, ",")));
            }
        }
    }
    
    @DataProvider(name = "resolveDomainDataProvider")
    private Object[][] resolveDomainDataProvider() {
        return new Object[][] {
                { new String[] { "Domain" }, new String[] { "Domain" } },
                { new String[] { "Domain", "Website", "Email" }, new String[] { "Domain", "Website", "Email" } },
                { new String[] { "Website", "Domain", "Email" }, new String[] { "Domain", "Website", "Email" } },
                { new String[] { "DomainName", "Website", "EmailAddress" },
                        new String[] { "Website" } },
        };
    }

    @Test(groups = "unit", dataProvider = "NameLocation")
    public void testEvalKeyLevel(String name, String countryCode, String state, String city, MatchKey expected) {
        MatchKeyTuple tuple = createMatchKeyTuple(name, countryCode, state, city);
        Assert.assertEquals(MatchKeyUtils.evalKeyLevel(tuple), expected);
    }

    @Test(groups = "unit", dataProvider = "MatchKeyLevel")
    public void testCompareKeyLevel(MatchKey compared, MatchKey compareTo, int expected) {
        Assert.assertEquals(MatchKeyUtils.compareKeyLevel(compared, compareTo), expected);
    }

    @Test(groups = "unit", dataProvider = "KeyPartition")
    public void testEvalKeyPartition(MatchKeyTuple tuple, String expectedKeyPartition) {
        String keyPartition = MatchKeyUtils.evalKeyPartition(tuple);
        if (expectedKeyPartition == null) {
            Assert.assertNull(keyPartition);
        } else {
            Assert.assertEquals(keyPartition, expectedKeyPartition);

            // make sure non name/location match key will not affect the result
            tuple = addNonNameLocationFields(tuple);
            keyPartition = MatchKeyUtils.evalKeyPartition(tuple);
            Assert.assertEquals(keyPartition, expectedKeyPartition);
        }
    }

    @Test(groups = "unit")
    public void testResolveKeyPosition() {
        List<String> fields = Arrays
                .asList(new String[] { MatchKey.Name.name(), MatchKey.Country.name(), MatchKey.State.name(),
                        MatchKey.City.name(), MatchKey.DUNS.name(), MatchKey.Domain.name(), MatchKey.Email.name(),
                        MatchKey.EntityId.name(), InterfaceName.AccountId.name(), "SFDC_ID", "MKTO_ID" });
        MatchInput input = new MatchInput();
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(fields);
        keyMap.put(MatchKey.SystemId, Arrays.asList(InterfaceName.AccountId.name(), "SFDC_ID", "MKTO_ID"));
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        entityKeyMap.setKeyMap(keyMap);
        input.setFields(fields);
        input.setKeyMap(keyMap);
        input.setEntityKeyMaps(Collections.singletonMap(BusinessEntity.Account.name(), entityKeyMap));

        Map<MatchKey, List<Integer>> keyPos = MatchKeyUtils.getKeyPositionMap(input);
        Assert.assertNotNull(keyPos);
        verifyResolvedKeyPos(fields, keyMap, keyPos);
        Map<String, Map<MatchKey, List<Integer>>> entityKeyPos = MatchKeyUtils.getEntityKeyPositionMaps(input);
        Assert.assertNotNull(entityKeyPos);
        Assert.assertNotNull(entityKeyPos.get(BusinessEntity.Account.name()));
        verifyResolvedKeyPos(fields, keyMap, entityKeyPos.get(BusinessEntity.Account.name()));
    }

    private void verifyResolvedKeyPos(List<String> fields, Map<MatchKey, List<String>> keyMap,
            Map<MatchKey, List<Integer>> keyPos) {
        Set<String> resolvedFldSet = new HashSet<>();
        Set<Integer> resolvedFldPosSet = new HashSet<>();
        Assert.assertTrue(MapUtils.isNotEmpty(keyMap));
        Assert.assertTrue(MapUtils.isNotEmpty(keyPos));
        Assert.assertEquals(keyMap.size(), keyPos.size());
        for (Map.Entry<MatchKey, List<String>> ent : keyMap.entrySet()) {
            List<String> keyFields = ent.getValue();
            List<Integer> keyIndexes = keyPos.get(ent.getKey());
            Assert.assertTrue(CollectionUtils.isNotEmpty(keyFields));
            Assert.assertTrue(CollectionUtils.isNotEmpty(keyIndexes));
            Assert.assertEquals(keyFields.size(), keyIndexes.size());
            for (int i = 0; i < keyFields.size(); i++) {
                // Verify no duplicate
                Assert.assertFalse(resolvedFldSet.contains(keyFields.get(i)));
                resolvedFldSet.add(keyFields.get(i));
                Assert.assertFalse(resolvedFldPosSet.contains(keyIndexes.get(i)));
                resolvedFldPosSet.add(keyIndexes.get(i));
                // Verify correctness of field position
                Assert.assertEquals(keyFields.get(i), fields.get(keyIndexes.get(i)));
            }
        }
        // Verify all the field positions are resolved
        Assert.assertEquals(resolvedFldSet.size(), fields.size());
        Assert.assertEquals(resolvedFldPosSet.size(), fields.size());
    }

    // name, countrycode, state, city, expected match key level
    @DataProvider(name = "NameLocation")
    private Object[][] provideNameLocation() {
        return new Object[][] { //
                { "Name", "Country", "State", "City", MatchKey.City }, //
                { "Name", "Country", null, "City", MatchKey.City }, //
                { "Name", "Country", "State", null, MatchKey.State }, //
                { "Name", "Country", null, null, MatchKey.Country }, //
                { "Name", null, null, null, MatchKey.Name }, //
                { null, null, null, null, null }, //
        };
    }

    private MatchKeyTuple createMatchKeyTuple(String name, String countryCode, String state, String city) {
        MatchKeyTuple tuple = new MatchKeyTuple();
        tuple.setName(name);
        tuple.setCountryCode(countryCode);
        tuple.setState(state);
        tuple.setCity(city);
        return tuple;
    }

    private MatchKeyTuple addNonNameLocationFields(MatchKeyTuple tuple) {
        tuple.setDuns("100000000");
        tuple.setZipcode("94404");
        tuple.setPhoneNumber("6690123456");
        tuple.setEmail("johndoe@lattice-engines.com");
        return tuple;
    }

    // compared, compareTo, expected result
    @DataProvider(name = "MatchKeyLevel")
    private Object[][] provideMatchKeyLevel() {
        return new Object[][] {
                { MatchKey.Name, MatchKey.Name, 0 }, //
                { MatchKey.Name, MatchKey.Country, -1 }, //
                { MatchKey.Name, MatchKey.State, -1 }, //
                { MatchKey.Name, MatchKey.City, -1 }, //
                { MatchKey.Country, MatchKey.Name, 1 }, //
                { MatchKey.Country, MatchKey.Country, 0 }, //
                { MatchKey.Country, MatchKey.State, -1 }, //
                { MatchKey.Country, MatchKey.City, -1 }, //
                { MatchKey.State, MatchKey.Name, 1 }, //
                { MatchKey.State, MatchKey.Country, 1 }, //
                { MatchKey.State, MatchKey.State, 0 }, //
                { MatchKey.State, MatchKey.City, -1 }, //
                { MatchKey.City, MatchKey.Name, 1 }, //
                { MatchKey.City, MatchKey.Country, 1 }, //
                { MatchKey.City, MatchKey.State, 1 }, //
                { MatchKey.City, MatchKey.City, 0 }, //
        };
    }

    // MatchKeyTuple, expectedKeyPartition
    @DataProvider(name = "KeyPartition")
    private Object[][] provideKeyPartition() {
        return new Object[][] {
                {
                        createMatchKeyTuple("Name", "Country", "State", "City"),
                        "City,Country,Name,State"
                },
                {
                        createMatchKeyTuple(null, "Country", "State", "City"),
                        "City,Country,State"
                },
                {
                        createMatchKeyTuple("Name", null, "State", "City"),
                        "City,Name,State"
                },
                {
                        createMatchKeyTuple("Name", "Country", null, "City"),
                        "City,Country,Name"
                },
                {
                        createMatchKeyTuple("Name", "Country", "State", ""),
                        "Country,Name,State"
                },
                {
                        createMatchKeyTuple("", "", "", ""),
                        null
                },
        };
    }

}
