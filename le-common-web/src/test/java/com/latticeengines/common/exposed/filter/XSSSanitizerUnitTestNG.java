package com.latticeengines.common.exposed.filter;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class XSSSanitizerUnitTestNG {

    @BeforeClass(groups = { "unit", "functional" })
    public void setup() throws Exception {
    }

    @AfterClass(groups = { "unit", "functional" })
    public void tearDown() {
    }

    @Test(groups = "unit")
    public void testSanitize() throws Exception {
        String antiSamyPath = XSSSanitizerUnitTestNG.class.getClassLoader()
                .getResource( "antisamy-policy.xml").getFile();
        XSSSanitizer sanitizer = new XSSSanitizer(antiSamyPath);
        String result = sanitizer.Sanitize("Test<script>alert('attack!!!')</script>");
        Assert.assertEquals(result, "Test");
    }
}
