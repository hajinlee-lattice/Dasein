package com.latticeengines.common.exposed.filter;

import org.springframework.core.io.ClassPathResource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class XSSSanitizerUnitTestNG {

    @BeforeClass(groups = { "unit", "functional" })
    public void setup() throws Exception {
    }

    @AfterClass(groups = { "unit", "functional" })
    public void tearDown() {
    }

    @Test(groups = "unit")
    public void testSanitize() throws Exception {
        String antiSamyPath = new ClassPathResource("antisamy-policy.xml").getFile().getPath();
        XSSSanitizer sanitizer = new XSSSanitizer(antiSamyPath);
        String result = sanitizer.Sanitize("Test<script>alert('attack!!!')</script>");
        Assert.assertEquals(result, "Test");
    }

    @Test(groups = "unit")
    public void testSanitize2() throws Exception {
        String antiSamyPath = new ClassPathResource("antisamy-policy.xml").getFile().getPath();
        XSSSanitizer sanitizer = new XSSSanitizer(antiSamyPath);
        String result = sanitizer.Sanitize("Test<javascript>alert('attack!!!')</javascript>");
        Assert.assertEquals(result, "Testalert('attack!!!')");
    }
}
