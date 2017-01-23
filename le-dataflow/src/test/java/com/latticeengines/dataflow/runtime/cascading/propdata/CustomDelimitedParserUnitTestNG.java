package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CustomDelimitedParserUnitTestNG {
    // example 1: abc="fff"|def="ddd",pqr => abc="fff",pqr
    // example 1: abc="ff,f"|def="ddd",pqr => abc="ff,f",pqr
    // example 1: abc="fff"|def="ddd" => abc="fff"

    @Test(groups = "unit")
    public void testPreprocess1() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc=\"fff\"|def=\"ddd\",pqr"), "abc=\"fff\",pqr");
    }

    @Test(groups = "unit")
    public void testPreprocess2() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc=\"ff,f\"|def=\"ddd\",pqr"), "abc=\"ff,f\",pqr");
    }

    @Test(groups = "unit")
    public void testPreprocess3() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc=\"fff\"|def=\"ddd\""), "abc=\"fff\"");
    }

    @Test(groups = "unit")
    public void testPreprocess4() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc,ddd,eee"), "abc,ddd,eee");
    }

    @Test(groups = "unit")
    public void testPreprocess5() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc,\"ddd,eee\",fff"), "abc,\"ddd,eee\",fff");
    }

    @Test(groups = "unit")
    public void testPreprocess6() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc,d\"dd,eee,ff\"f"), "abc,d\"dd,eee,ff\"f");
    }

    @Test(groups = "unit")
    public void testPreprocess7() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc,d=\"d,d\",eee,ff\"f"),
                "abc,d=\"d,d\",eee,ff\"f");
    }

    @Test(groups = "unit")
    public void testPreprocess8() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc,d=\"d,d\",eee,ff=\"f\""),
                "abc,d=\"d,d\",eee,ff=\"f\"");
    }

    @Test(groups = "unit")
    public void testPreprocess9() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue("abc,d=\"d,d\",eee,ff\"f,"),
                "abc,d=\"d,d\",eee,ff\"f,");
    }

    @Test(groups = "unit")
    public void testPreprocess10() {
        Assert.assertEquals(CustomDelimitedParserSpecialEqualQuote.preProcessValue(",abc,d=\"d,d\",eee,ff=\"f\""),
                ",abc,d=\"d,d\",eee,ff=\"f\"");
    }}
