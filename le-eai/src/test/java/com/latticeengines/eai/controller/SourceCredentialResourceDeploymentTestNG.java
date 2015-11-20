package com.latticeengines.eai.controller;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;

public class SourceCredentialResourceDeploymentTestNG extends EaiFunctionalTestNGBase {

    @Value("${eai.test.service.url}")
    private String url;

    @Value("${eai.test.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.test.salesforce.password}")
    private String salesforcePasswd;

    @Value("${eai.test.salesforce.securitytoken}")
    private String salesforceSecurityToken;

    @Value("${eai.salesforce.production.loginurl}")
    private String productionLoginUrl;

    @Value("${eai.salesforce.sandbox.loginurl}")
    private String sandboxLoginUrl;

    @Autowired
    private RestTemplate restTemplate;

    @BeforeClass(groups = "deployment")
    private void setup() throws Exception {
        url = url + "/validatecredential/customerspaces/{customerSpace}/sourcetypes/{sourceType}";
    }

    // test production
    @Test(groups = "deployment", enabled = true)
    public void testValidCredential() throws Exception {
        CrmCredential cred = new CrmCredential();
        cred.setUserName(salesforceUserName);
        cred.setPassword(CipherUtils.encrypt(salesforcePasswd));
        cred.setSecurityToken(salesforceSecurityToken);
        cred.setUrl(productionLoginUrl);
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", "somecustomer");
        uriVariables.put("sourceType", CrmConstants.CRM_SFDC);
        SimpleBooleanResponse response = restTemplate.postForObject(url, cred, SimpleBooleanResponse.class,
                uriVariables);
        System.out.println(response);
        assertTrue(response.isSuccess());
    }

    // test sandbox
    @Test(groups = "deployment", enabled = true)
    public void testValidSandboxCredential() throws Exception {
        CrmCredential cred = new CrmCredential();
        cred.setUserName("tsanghavi@lattice-engines.com.sandbox2");
        cred.setPassword(CipherUtils.encrypt("Happy2010"));
        cred.setSecurityToken("5aGieJUACRPQ21CG3nUwn8iz");
        cred.setUrl(sandboxLoginUrl);
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", "somecustomer");
        uriVariables.put("sourceType", CrmConstants.CRM_SFDC);
        SimpleBooleanResponse response = restTemplate.postForObject(url, cred, SimpleBooleanResponse.class,
                uriVariables);
        System.out.println(response);
        assertTrue(response.isSuccess());
    }

    @Test(groups = "deployment", enabled = true)
    public void testInvalidCredential() throws Exception {
        CrmCredential cred = new CrmCredential();
        cred.setUserName(salesforceUserName);
        cred.setPassword(salesforcePasswd + "ab");
        cred.setUrl(productionLoginUrl);
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", "somecustomer");
        uriVariables.put("sourceType", CrmConstants.CRM_SFDC);
        SimpleBooleanResponse response = restTemplate.postForObject(url, cred, SimpleBooleanResponse.class,
                uriVariables);
        System.out.println(response);
        assertFalse(response.isSuccess());
    }
}
