package com.latticeengines.common.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class LocationUtilsUnitTestNG {

    private static Logger log = LoggerFactory.getLogger(LocationUtilsUnitTestNG.class);

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
                { "USA", "District Of Columbia  ", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "United States", "  Dist. of Columbia", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "U.S.", "D.C.", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "U.S.A.", "D. C.", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "U. S. A.", "DC-District Of Typo", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "USA", "DC - Dist. of Columbia", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "USA", "D.C. - Dist. of Columbia-", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "USA", "D. C. - Dist. of Columbia", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "USA", "Washington D.C.", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "USA", "Washington D. C.", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "USA", "Wash. D. C.", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "USA", "Wash. D.C.", "WASHINGTON DC", "SOUTH ATLANTIC" }, //
                { "USA", "Washington", "WASHINGTON", "PACIFIC" }, //
                { "USA", "New York", "NEW YORK", "MID ATLANTIC" }, //
                { "USA", "New Mexico", "NEW MEXICO", "MOUNTAIN" }, //
                { "USA", "FL - Flori", "FLORIDA", "SOUTH ATLANTIC" }, //
                { "USA", "Rhode Island", "RHODE ISLAND", "NEW ENGLAND" }, //
                { "USA", "Virgin Islands", "VIRGIN ISLANDS", "OTHER" }, //
                { "USA", "NB", "NEBRASKA", "WEST NORTH CENTRAL" }, //
                { "Canada", "NB", "NEW BRUNSWICK", "CANADA" }, //
                { "Canada", "Québec", "QUEBEC", "CANADA" }, //
                { "Canada", "ONTARIO", "ONTARIO", "CANADA" }, //
                { "China", "Beijing", "BEIJING", "CHINA" }, //
                { "Japan", "Nowhere", "NOWHERE", "JAPAN" }, //
                { "USA", "Beijing", null, "OTHER" }, //
                { "Canada", "Nowhere", null, "CANADA" }, //
        };
    }

}
