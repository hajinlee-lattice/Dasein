package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

public class MatchKeyUtilsUnitTestNG {
    
    @Test(groups = "unit", dataProvider = "NameLocation")
    public void testEvalKeyLevel(String name, String countryCode, String state, String city, MatchKey expected) {
        MatchKeyTuple tuple = createMatchKeyTuple(name, countryCode, state, city);
        Assert.assertEquals(MatchKeyUtils.evalKeyLevel(tuple), expected);
    }

    @Test(groups = "unit", dataProvider = "MatchKeyLevel")
    public void testCompareKeyLevel(MatchKey compared, MatchKey compareTo, int expected) {
        Assert.assertEquals(MatchKeyUtils.compareKeyLevel(compared, compareTo), expected);
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
}
