package com.latticeengines.common.exposed.filter;

import java.io.InputStream;

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
        InputStream policy = new ClassPathResource("antisamy-policy.xml").getInputStream();
        XSSSanitizer sanitizer = new XSSSanitizer(policy);
        String result = sanitizer.Sanitize("Test<script>alert('attack!!!')</script>");
        Assert.assertEquals(result, "Test");
    }

    @Test(groups = "unit")
    public void testSanitize2() throws Exception {
        InputStream policy = new ClassPathResource("antisamy-policy.xml").getInputStream();
        XSSSanitizer sanitizer = new XSSSanitizer(policy);
        String result = sanitizer.Sanitize("Test<javascript>alert('attack!!!')</javascript>");
        Assert.assertEquals(result, "Testalert('attack!!!')");
    }
}
