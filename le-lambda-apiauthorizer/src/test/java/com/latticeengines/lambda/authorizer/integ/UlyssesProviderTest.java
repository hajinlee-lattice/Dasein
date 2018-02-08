package com.latticeengines.lambda.authorizer.integ;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.lambda.authorizer.AbstractApiAuthorizerTest;
import com.latticeengines.lambda.authorizer.domain.GatewayPolicyConfiguration;
import com.latticeengines.lambda.authorizer.domain.TokenAuthorizerContext;


public class UlyssesProviderTest extends AbstractApiAuthorizerTest {

    @Autowired
    private UlyssesProvider ulyssesProvider;
    
    @Test
    public void testUlyssesHealth() {
        Assert.assertNotNull(ulyssesProvider);
        
        ResponseEntity<String> response = ulyssesProvider.getHealth();
        
        System.out.println("Status Code: " + response.getStatusCode());
        System.out.println("Body: " + response.getBody());
    }
    
    @Test(dependsOnMethods = "testUlyssesHealth")
    public void testGetGatewayPolicyConfig() {
        Assert.assertNotNull(ulyssesProvider);

        TokenAuthorizerContext tokenContext = new TokenAuthorizerContext();
        tokenContext.setAuthorizationToken("Bearer e1039cc1-b743-4cf4-be33-b7e6cc2108ff");
        GatewayPolicyConfiguration policyConfig = null;
        
        policyConfig = ulyssesProvider.getGatewayPolicyConfig(tokenContext);
        Assert.assertNotNull(policyConfig);
        Assert.assertNotNull(policyConfig.getApiKey());
        
        System.out.println("API_KEY: " + policyConfig.getApiKey());
        System.out.println("Principal: " + policyConfig.getPrincipal());
    }
    
}
