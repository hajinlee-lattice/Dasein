package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MatchKeyTupleUnitTestNG {

    @Test(groups = "unit", dataProvider = "matchKeyTupleProvider")
    public void testIsDomainOnly(String domain, String name, String country, String phoneNumber, String duns,
            String email, List<Pair<String, String>> systemIds, boolean isDomainOnly) {
        MatchKeyTuple tuple = new MatchKeyTuple.Builder() //
                .withDomain(domain) //
                .withName(name) //
                .withCountry(country) //
                .withPhoneNumber(phoneNumber) //
                .withDuns(duns) //
                .withEmail(email) //
                .withSystemIds(systemIds)
                .build();
        Assert.assertEquals(tuple.hasDomainOnly(), isDomainOnly);
    }

    // Schema: Domain, Name, Country, PhoneNumber, Duns, Email, SystemIds,
    // IsDomainOnly
    @DataProvider(name = "matchKeyTupleProvider")
    private Object[][] matchKeyTupleProvider() {
        return new Object[][] {
                // Null Domain
                { null, null, null, null, null, null, null, false }, //
                { null, "google", null, null, null, null, null, false }, //
                { null, null, "usa", null, null, null, null, false }, //
                { null, null, null, "111-111-1111", null, null, null, false }, //
                { null, null, null, null, "123456789", null, null, false }, //
                { null, null, null, null, null, "a@gmail.com", null, false }, //
                { null, null, null, null, null, null, Arrays.asList(Pair.of("AID", "1")), false }, //
                // Domain only
                { "google.com", null, null, null, null, null, null, true }, //
                { "google.com", null, null, null, null, null, Collections.emptyList(), true }, //
                // Multi-fields populated
                { "google.com", "google", null, null, null, null, null, false }, //
                { "google.com", null, "usa", null, null, null, null, false }, //
                { "google.com", null, null, "111-111-1111", null, null, null, false }, //
                { "google.com", null, null, null, "123456789", null, null, false }, //
                { "google.com", null, null, null, null, "a@gmail.com", null, false }, //
                { "google.com", null, null, null, null, null, Arrays.asList(Pair.of("AID", "1")), false }, //

        };
    }
}
