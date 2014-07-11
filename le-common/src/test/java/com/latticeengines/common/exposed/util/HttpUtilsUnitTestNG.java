package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertTrue;

import org.codehaus.jackson.annotate.JsonProperty;
import org.testng.annotations.Test;

public class HttpUtilsUnitTestNG {
    
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
    
    @Test(groups = "unit")
    public void testExecutePostRequestNormalTransport() throws Exception {
        String result = HttpUtils.executePostRequest("http://httpbin.org/post", request, null);
        assertTrue(result.contains("httpbin.org"));
        
    }

    @Test(groups = "unit")
    public void testExecutePostRequestSSLTransport() throws Exception {
        String result = HttpUtils.executePostRequest("https://httpbin.org/post", request, null);
        assertTrue(result.contains("httpbin.org"));
    }
}
