package com.latticeengines.security.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class InternalResourceTestNG extends SecurityFunctionalTestNGBase {

    @Test(groups = "functional")
    public void accessWithoutHeader() {
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
        boolean exception = false;
        try {
            restTemplate.getForObject(getRestAPIHostPort() + "/internal/resource", Map.class, new HashMap<>());
        } catch (Exception e) {
            exception = true;
            String code = e.getMessage();
            assertEquals(code, "401 UNAUTHORIZED");
        }
        assertTrue(exception);
    }

    @Test(groups = "functional")
    public void accessHealthCheck() {
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        StatusDocument statusDocument = restTemplate.getForObject(getRestAPIHostPort() + "/internal/health", StatusDocument.class);
        Assert.assertNotNull(statusDocument);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void accessWithHeader() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Collections.singletonList(addMagicAuthHeader));
        Map<String, String> retVal = magicRestTemplate.getForObject(getRestAPIHostPort() + "/internal/resource",
                Map.class, new HashMap<>());
        assertTrue(retVal.containsKey("SomeInternalValue"));
    }

}
