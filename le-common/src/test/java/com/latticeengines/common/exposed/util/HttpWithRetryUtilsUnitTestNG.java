package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HttpWithRetryUtilsUnitTestNG {

    private static class Request {

        Request(String a, String b) {
            this.a = a;
            this.b = b;
        }

        @JsonProperty("a")
        String a;

        @JsonProperty("b")
        String b;
    }

    private Request request = new Request("abc", "xyz");

    @SuppressWarnings("deprecation")
    @Test(groups = "unit")
    public void testExecutePostRequestNormalTransport() throws Exception {
        String result = HttpWithRetryUtils.executePostRequest("http://httpbin.org/post", request, null);
        assertTrue(result.contains("httpbin.org"));

    }

    @SuppressWarnings("deprecation")
    @Test(groups = "unit", enabled = false)
    public void testExecutePostRequestSSLTransport() throws Exception {
        String result = HttpWithRetryUtils.executePostRequest("https://httpbin.org/post", request, null);
        assertTrue(result.contains("httpbin.org"));
    }
}
