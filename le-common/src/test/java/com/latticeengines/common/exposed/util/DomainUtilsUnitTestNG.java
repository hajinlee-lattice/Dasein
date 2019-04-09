package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DomainUtilsUnitTestNG {
    @Test(groups = "unit", dataProvider = "domainDataProvider")
    public void testParseDomain(String url, String domain) {
        Assert.assertEquals(DomainUtils.parseDomain(url), domain);
    }

    @DataProvider(name = "domainDataProvider")
    Object[][] domainDataProvider() {
        return new Object[][]{
                {"google.com", "google.com"},
                {"http://www.hugedomains.com/domain_profile.cfm?d=whitesidedesigns&e=com", "hugedomains.com"},
                {"http://www.rutherfordproperty.co.nz", "rutherfordproperty.co.nz"},
                {"http://trinid.com/", "trinid.com"},
                {"maps.google.com", "maps.google.com"},
                {"adoic@gmail.com", "gmail.com"},
                {"greg.perrott@rbnz.govt.nz", "rbnz.govt.nz"},
                {"greg@perrott@rbnz.govt.nz", "rbnz.govt.nz"},
                {"www.www.com", "www.com"},
                {"abcdefg", null},
                {"www.www", null},
                {"@", null},
                {"greg.perrott@", null},
                {"greg.perrott@domain", null},
                { "www.abc.technology", "abc.technology" },
                { "www.abc.com.technology", "abc.com.technology" },
        };
    }

    @Test(groups = "unit", dataProvider = "emailDataProvider")
    public void testParseEmail(String rawEmail, String parsedEmail) {
        Assert.assertEquals(DomainUtils.parseEmail(rawEmail), parsedEmail);
    }

    @DataProvider(name = "emailDataProvider")
    private Object[][] emailDataProvider() {
        // Schema: Raw Email, Parsed Email
        return new Object[][] {
                // Positive Cases
                { "aa@gmail.com", "aa@gmail.com" }, //
                { " aa@gmail.com ", "aa@gmail.com" }, //
                { "  a  a  @  gm  ail   . com ", "aa@gmail.com" }, //
                { "www.aa@gmail.com", "www.aa@gmail.com" }, //
                { "www-aa@lattice-engines.com", "www-aa@lattice-engines.com" }, //
                { "aa@sina.com.cn", "aa@sina.com.cn" }, //
                { "aa@att.net", "aa@att.net" }, //
                // Not able to validate domain extension (expected for now)
                { "aa@gmail.aaa", "aa@gmail.aaa" }, //

                // Negative Cases
                { "gmail.com", null }, //
                { "@gmail.com", null }, //
                { "aa@gmail", null }, //
                { "aa@@gmail.com", null }, //
                { "aa@.gmail.com", null }, //
                { "aa@gmail..com", null }, //
                { "aa@@gmail.com.", null }, //

        };
    }
}
