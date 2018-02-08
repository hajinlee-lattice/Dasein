package com.latticeengines.lambda.authorizer.handler;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.latticeengines.lambda.authorizer.AbstractApiAuthorizerTest;
import com.latticeengines.lambda.authorizer.domain.AuthPolicy;
import com.latticeengines.lambda.authorizer.domain.TokenAuthorizerContext;

public class APIGatewayAuthorizerHandlerTest extends AbstractApiAuthorizerTest {
    
    private static final String VALID_ACCESS_TOKEN_EMPTY_API_KEY = "Bearer 66617b22-3bee-4282-94f5-e5c05ee3027e";

    private static final String INVALID_ACCESS_TOKEN = "Bearer BadToken";

    private static final String ARN_METHOD = "arn:aws:execute-api:us-east-1:028036828464:3iy8svxpcc/*/GET/enrichment/categories";

    private static final String VALID_ACCESS_TOKEN_WITH_API_KEY = "Bearer e1039cc1-b743-4cf4-be33-b7e6cc2108ff";

    private static Logger LOG = Logger.getLogger(APIGatewayAuthorizerHandlerTest.class);
    
    @Autowired
    private RequestHandler<TokenAuthorizerContext, AuthPolicy> gatewayAuthorizerHandler;
    
    @Test
    public void testGetGatewayPolicyConfigValidAuthorization() {
        Assert.assertNotNull(gatewayAuthorizerHandler);

        TokenAuthorizerContext tokenContext = new TokenAuthorizerContext();
        tokenContext.setAuthorizationToken(VALID_ACCESS_TOKEN_WITH_API_KEY);
        tokenContext.setMethodArn(ARN_METHOD);
        
        AuthPolicy authPolicy = gatewayAuthorizerHandler.handleRequest(tokenContext, null);
        
        Assert.assertNotNull(authPolicy);
        assertAuthPolicyStatements(authPolicy, 1, 0);
        Assert.assertNotNull(authPolicy.getPrincipalId());
        Assert.assertNotNull(authPolicy.getUsageIdentifierKey());
    }

    protected void assertAuthPolicyStatements(AuthPolicy authPolicy, int allowResourcesCnt, int denyResourceCnt) {
        Assert.assertNotNull(authPolicy.getPolicyDocument());
        Assert.assertNotNull(authPolicy.getPolicyDocument().get(AuthPolicy.STATEMENT));
        
        Map<String, Object>[] statementMap = (Map<String, Object>[]) authPolicy.getPolicyDocument().get(AuthPolicy.STATEMENT);
        Assert.assertEquals(statementMap.length, 2);
        for (Map<String, Object> statement : statementMap) {
            String[] resources = (String[]) statement.get(AuthPolicy.RESOURCE);

            switch ((String) statement.get(AuthPolicy.EFFECT)) {
            case "Allow":
                Assert.assertTrue(resources.length == allowResourcesCnt);
                break;
            case "Deny":
                Assert.assertTrue(resources.length == denyResourceCnt);
                break;
            default:
                Assert.fail("Illegal Statement Effect");
            }
        }
    }
    
    @Test
    public void testGetGatewayPolicyConfigInvalidToken() {
        Assert.assertNotNull(gatewayAuthorizerHandler);

        TokenAuthorizerContext tokenContext = new TokenAuthorizerContext();
        tokenContext.setAuthorizationToken(INVALID_ACCESS_TOKEN);
        tokenContext.setMethodArn(ARN_METHOD);
        
        AuthPolicy authPolicy = gatewayAuthorizerHandler.handleRequest(tokenContext, null);
        
        Assert.assertNotNull(authPolicy);
        assertAuthPolicyStatements(authPolicy, 0, 1);
        Assert.assertNull(authPolicy.getPrincipalId());
        Assert.assertNull(authPolicy.getUsageIdentifierKey());
    }
    
    @Test
    public void testGetGatewayPolicyConfigEmptyApiKey() {
        Assert.assertNotNull(gatewayAuthorizerHandler);

        TokenAuthorizerContext tokenContext = new TokenAuthorizerContext();
        tokenContext.setAuthorizationToken(VALID_ACCESS_TOKEN_EMPTY_API_KEY);
        tokenContext.setMethodArn(ARN_METHOD);
        
        AuthPolicy authPolicy = gatewayAuthorizerHandler.handleRequest(tokenContext, null);
        
        Assert.assertNotNull(authPolicy);
        assertAuthPolicyStatements(authPolicy, 1, 0);
        Assert.assertNotNull(authPolicy.getPrincipalId());
        Assert.assertNull(authPolicy.getUsageIdentifierKey());
    }
    
}
