package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class LocationUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "usaDataProvider")
    public void testParseUSA(String country) {
        String standardCountry = LocationUtils.getStandardCountry(country);
        Assert.assertEquals(standardCountry, LocationUtils.USA);
    }

    @DataProvider(name = "usaDataProvider")
    Object[][] usaDataProvider() {
        return new Object[][] { //
                { LocationUtils.USA }, //
                { "USA" }, //
                { "U.S.A." }, //
                { "U. S. A." }, //
                { "U S A" }, //
                { "U.S." }, //
                { "US." }, //
                { "U S" }, //
                { " U S" }, //
                { "United States" }, //
                { "United States of America" }, //
                { "united states of america" }, //
                { "America" }, //
        };
    }

    @Test(groups = "unit", dataProvider = "stateDataProvider")
    public void testParseState(String country, String state, String expectedState, String expectedRegion) {
        String standardCountry = LocationUtils.getStandardCountry(country);
        String standardState = LocationUtils.getStandardState(standardCountry, state);
        String region = LocationUtils.getStandardRegion(standardCountry, standardState);
        Assert.assertEquals(standardState, expectedState);
        Assert.assertEquals(region, expectedRegion);
    }

    @DataProvider(name = "stateDataProvider")
    Object[][] stateDataProvider() {
        return new Object[][] { //
                { "USA", "District Of Columbia  ", "Washington D.C.", "South Atlantic" }, //
                { "United States", "  Dist. of Columbia", "Washington D.C.", "South Atlantic" }, //
                { "U.S.", "D.C.", "Washington D.C.", "South Atlantic" }, //
                { "U.S.A.", "D. C.", "Washington D.C.", "South Atlantic" }, //
                { "U. S. A.", "DC-District Of Typo", "Washington D.C.", "South Atlantic" }, //
                { "USA", "DC - Dist. of Columbia", "Washington D.C.", "South Atlantic" }, //
                { "USA", "D.C. - Dist. of Columbia-", "Washington D.C.", "South Atlantic" }, //
                { "USA", "D. C. - Dist. of Columbia", "Washington D.C.", "South Atlantic" }, //
                { "USA", "Washington D.C.", "Washington D.C.", "South Atlantic" }, //
                { "USA", "Washington D. C.", "Washington D.C.", "South Atlantic" }, //
                { "USA", "Wash. D. C.", "Washington D.C.", "South Atlantic" }, //
                { "USA", "Wash. D.C.", "Washington D.C.", "South Atlantic" }, //
                { "USA", "Washington", "Washington", "Pacific" }, //
                { "USA", "New York", "New York", "Mid-Atlantic" }, //
                { "USA", "New Mexico", "New Mexico", "Mountain" }, //
                { "USA", "FL - Flori", "Florida", "South Atlantic" }, //
                { "USA", "Rhode Island", "Rhode Island", "New England" }, //
                { "USA", "Virgin Islands", "Virgin Islands", "Other" }, //
                { "USA", "NB", "Nebraska", "West North Central" }, //
                { "Canada", "NB", "New Brunswick", "Canada" }, //
                { "Canada", "Québec", "Quebec", "Canada" }, //
                { "China", "Beijing", "Beijing", "China" }, //
                { "Japan", "Nowhere", "Nowhere", "Japan" }, //
                { "USA", "Beijing", null, "Other" }, //
                { "Canada", "Nowhere", null, "Canada" }, //
        };
    }

}
