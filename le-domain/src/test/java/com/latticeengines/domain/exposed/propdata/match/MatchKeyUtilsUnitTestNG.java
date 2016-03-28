package com.latticeengines.domain.exposed.propdata.match;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

}
