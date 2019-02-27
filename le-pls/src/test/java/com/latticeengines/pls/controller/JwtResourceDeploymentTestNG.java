package com.latticeengines.pls.controller;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.springframework.web.util.UriComponentsBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class JwtResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String USER_EMAIL = "ron@lattice-engines.com";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        deleteUserByRestCall(USER_EMAIL);
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() {

    }

    @Test(groups = { "deployment" })
    public void getJwtToken() {

        Map<String, String> testInput = new HashMap<String, String>();
        testInput.put("return_to", "http://latticeengines.zendesk.com");
        testInput.put("source_ref", "zendesk");

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("requestParameters", testInput);
        URI attrUrl = UriComponentsBuilder.fromUriString(getRestAPIHostPort() + "/pls/jwt/handle_request").build()
                .toUri();
        String view = restTemplate.postForObject(attrUrl, result, String.class);
    }
}
