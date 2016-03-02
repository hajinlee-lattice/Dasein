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
                {"www.www.com", "www.com"},
                {"abcdefg", null},
                {"www.www", null}
        };
    }
}
