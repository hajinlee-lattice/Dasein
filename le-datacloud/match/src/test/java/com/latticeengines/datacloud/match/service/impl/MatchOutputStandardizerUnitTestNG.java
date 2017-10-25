package com.latticeengines.datacloud.match.service.impl;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MatchOutputStandardizerUnitTestNG {
    private final Object[] TEST_DATA = { 123L, "abc.com", "abc.com\nabc123.com", "abc.com,\r\nabc123.com",
            "abc.com,\r abc123.com", "abc.com, abc123.com", "abc.com,abc123.com"
    };

    @Test(groups = "unit")
    public void testcleanNewlineCharacters() {
        for (Object obj : TEST_DATA) {
            Object result = MatchOutputStandardizer.cleanNewlineCharacters(obj);
            if (obj instanceof String) {
                String objString = (String) obj;
                String resultString = (String) result;
                if (objString.matches("(\r\n|\r|\n)s+")) {
                    Assert.assertFalse(resultString.contains("\r"));
                    Assert.assertFalse(resultString.contains("\n"));
                    Assert.assertFalse(resultString.contains("\r\n"));
                }
            } else {
                Assert.assertEquals(result, obj);
            }
        }
    }
}
