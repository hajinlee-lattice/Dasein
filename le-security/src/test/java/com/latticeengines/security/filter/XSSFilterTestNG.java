package com.latticeengines.security.filter;

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

@Test
public class XSSFilterTestNG {

    private JsonNode cases = null;

    @BeforeClass(groups = { "unit", "functional" })
    public void setup() throws Exception {
        try(InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("xss_sanitize.json")) {
            String testData = IOUtils.toString(input, StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            cases = mapper.readTree(testData);
        }
    }

    @AfterClass(groups = { "unit", "functional" })
    public void tearDown() {
    }

    @Test(groups = "unit")
    public void testSanitize() {
        try {
            Method method = XSSFilter.FilteredRequest.class.getDeclaredMethod("sanitize", new Class[] {String.class});
            method.setAccessible(true);

            for(JsonNode tc : cases) {
                String result = (String)method.invoke(null, new Object[] {tc.get("input").asText()});
                Assert.assertEquals(result, tc.get("expect").asText());
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
}

