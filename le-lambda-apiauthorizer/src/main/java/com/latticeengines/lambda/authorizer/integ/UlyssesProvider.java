package com.latticeengines.lambda.authorizer.integ;

import org.apache.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import com.latticeengines.lambda.authorizer.domain.GatewayPolicyConfiguration;
import com.latticeengines.lambda.authorizer.domain.TokenAuthorizerContext;

@Component
public class UlyssesProvider extends AbstractProvider {

    private static Logger LOG = Logger.getLogger(UlyssesProvider.class);
    
    public ResponseEntity<String> getHealth() {
        ResponseEntity<String> response = restTemplate.getForEntity(authorizerConfig.getUlyssesBase() + "/health",
                String.class);
        return response;
    }
    
    public GatewayPolicyConfiguration getGatewayPolicyConfig(TokenAuthorizerContext tokenContext) {
        LOG.info("Authorization Token: " +tokenContext.getAuthorizationToken());

        GatewayPolicyConfiguration response = super.exchange("Get Gateway Config",
                authorizerConfig.getUlyssesBase() + "/api-gateway/tenant-config", HttpMethod.GET,
                new HttpEntity<>(getHttpHeaders(tokenContext)), GatewayPolicyConfiguration.class);
        
        return response;
    }

    protected HttpHeaders getHttpHeaders(TokenAuthorizerContext tokenContext) {
        HttpHeaders reqHeaders = new HttpHeaders();
        reqHeaders.set(HttpHeaders.AUTHORIZATION, tokenContext.getAuthorizationToken());
        return reqHeaders;
    }
    
}
