package com.latticeengines.eai.controller;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;

public class SourceCredentialResourceDeploymentTestNG extends EaiFunctionalTestNGBase{

    @Value("${eai.service.test.url}")
    private String url;

    @Value("${eai.salesforce.username}")
    private String salesforceUserName;

    @Value("${eai.salesforce.password}")
    private String salesforcePasswd;

    @Value("${eai.salesforce.production.loginurl}")
    private String productionLoginUrl;

    @Value("${eai.salesforce.sandbox.loginurl}")
    private String sandboxLoginUrl;

    @Autowired
    private RestTemplate restTemplate;

    @BeforeClass(groups = "functional")
    private void setup() throws Exception {
        url = url + "/customerspaces/{customerSpace}/validateCredential/{sourceType}";
    }

    //test production
    @Test(groups = "functional", enabled = true)
    public void testValidCredential(){
        CrmCredential cred = new CrmCredential();
        cred.setUserName(salesforceUserName);
        cred.setPassword(salesforcePasswd);
        cred.setUrl(productionLoginUrl);
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", "somecustomer");
        uriVariables.put("sourceType", CrmConstants.CRM_SFDC);
        SimpleBooleanResponse response = restTemplate.postForObject(url, cred, SimpleBooleanResponse.class, uriVariables);
        System.out.println(response);
        assertTrue(response.isSuccess());
    }

    //test sandbox
    @Test(groups = "functional", enabled = true)
    public void testValidSandboxCredential(){
        CrmCredential cred = new CrmCredential();
        cred.setUserName("tsanghavi@lattice-engines.com.sandbox2");
        cred.setPassword("Happy20105aGieJUACRPQ21CG3nUwn8iz");
        cred.setUrl(sandboxLoginUrl);
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", "somecustomer");
        uriVariables.put("sourceType", CrmConstants.CRM_SFDC);
        SimpleBooleanResponse response = restTemplate.postForObject(url, cred, SimpleBooleanResponse.class, uriVariables);
        System.out.println(response);
        assertTrue(response.isSuccess());
    }

    @Test(groups = "functional", enabled = true)
    public void testInvalidCredential(){
        CrmCredential cred = new CrmCredential();
        cred.setUserName(salesforceUserName);
        cred.setPassword(salesforcePasswd + "abc");
        cred.setUrl(productionLoginUrl);
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", "somecustomer");
        uriVariables.put("sourceType", CrmConstants.CRM_SFDC);
        SimpleBooleanResponse response = restTemplate.postForObject(url, cred, SimpleBooleanResponse.class, uriVariables);
        System.out.println(response);
        assertFalse(response.isSuccess());
    }
}
