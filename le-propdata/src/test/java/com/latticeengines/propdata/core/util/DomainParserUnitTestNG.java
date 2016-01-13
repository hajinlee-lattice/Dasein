package com.latticeengines.propdata.core.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.propdata.core.util.DomainParser;

public class DomainParserUnitTestNG {

    @Test(groups = {"unit"}, dataProvider = "parseDomainDataProvider")
    public void testParseDomain(String input, Set<String> output) {
        Set<String> domains = DomainParser.parseDomains(input);
        Assert.assertEquals(domains, output);
    }

    @DataProvider(name = "parseDomainDataProvider")
    Object[][] parseDomainDataProvider() {
        return new Object[][] {
                {
                        "\"http://members.aol.com/BrandonMaw/RDscreendesign.html\".RDScreendesign",
                        Collections.singleton("members.aol.com")
                },
                {
                        "(1) truinsight.com  (2) microenergetics.com (3) martianstargate.com",
                        new HashSet<>(Arrays.asList("(1)", "(2)", "(3)", "truinsight.com", "microenergetics.com", "martianstargate.com"))
                },
                {
                        ".com/,http://www.donnamistek.com/",
                        new HashSet<>(Arrays.asList(".com/", "donnamistek.com"))
                }
        };
    }

    @Test(groups = {"unit", "functional"}, dataProvider = "regexDataProvider")
    public void testParseByRegex(String input, String output) {
        String domain = DomainParser.parseDomainUsingRegex(input);
        Assert.assertEquals(domain, output);
    }

    @DataProvider(name = "regexDataProvider")
    Object[][] regexDataProvider() {
        return new Object[][] {
                {"\"http://members.aol.com/BrandonMaw/RDscreendesign.html\".RDScreendesign", "members.aol.com"},
                {"http://www.donnamistek.com/", "donnamistek.com"},
                {"truinsight.com", "truinsight.com"},
                {"//truinsight.com/", "truinsight.com"},
                {"(1)truinsight.com", "truinsight.com"},
                {"truinsight", "truinsight"},
                {":truinsight.com", "truinsight.com"},
                {"/instantmoneynetwork.com", "instantmoneynetwork.com"},
                {"(1)", "(1)"},
                {".com", ".com"}
        };
    }
}
