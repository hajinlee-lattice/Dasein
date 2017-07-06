package com.latticeengines.common.exposed.vdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RestAPIParserUnitTestNG {

    @Test(groups = "unit", enabled = false)
    public void testGetRestAPIResponse() throws IOException {
        String url = "http://10.41.1.187:8081/DLRestService/GetSpecDetails";
        Map<String, String> headers = new HashMap<>();
        Map<String, String> parameters = new HashMap<>();
        parameters.put("tenantName", "SFDC_2Checkout_POC");
        parameters.put("specName", "Version");
        headers.put("MagicAuthentication", "Security through obscurity!");
        headers.put("charset", "utf-8");

        String result = RestAPIParser.getRestAPIResponse(url, headers, parameters);
        Assert.assertNotNull(result);
    }

    @Test(groups = "unit", enabled = false)
    public void testGetSpecDetails() throws Exception {
        String url = "http://10.41.1.187:8081/DLRestService/GetSpecDetails";
        Map<String, String> headers = new HashMap<>();
        Map<String, String> parameters = new HashMap<>();
        parameters.put("tenantName", "SFDC_2Checkout_POC");
        parameters.put("specName", "Version");
        headers.put("MagicAuthentication", "Security through obscurity!");
        headers.put("charset", "utf-8");

        String result = RestAPIParser.getSpecDetails(url, headers, parameters);

        Assert.assertNotNull(result);

        SpecParser sp = new SpecParser(result);
        Assert.assertTrue(sp.getTemplate().contains("PLS SFDC Template"));
    }
}
