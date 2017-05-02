package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

@Component
public class SystemStatusDeploymentTest extends PlsDeploymentTestNGBase {
    private static final Log log = LogFactory.getLog(SystemStatusDeploymentTest.class);

    private static final String SYSTEMSTATUS_URL = "/pls/health/systemstatus";

    @Test(groups = "deployment")
    public void testSystemStatus() {
        String urlValue = getRestAPIHostPort() + SYSTEMSTATUS_URL;
        String url = restTemplate.getForObject(urlValue, String.class);
        log.info("url value : " + urlValue);
        assertTrue(StringUtils.isNotEmpty(url));
    }
}
