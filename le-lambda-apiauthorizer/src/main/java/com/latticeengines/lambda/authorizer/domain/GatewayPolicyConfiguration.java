package com.latticeengines.lambda.authorizer.domain;

/**
 * This class is duplicate of le-domain/com.latticeengines.domain.exposed.ulysses.GatewayPolicyConfiguration.
 * As I donot want to include entire le-domain into AWS lambda, I have copied this file manually.
 * If we have too many dependencies between lambda functions and le code base, will consider refactoring these classes to some common project.
 *  
 * @author jadusumalli
 */
public class GatewayPolicyConfiguration {

    private String principal;
    
    private String apiKey;

    //Can add allowed roles / policy configurations that can consumed on Gateway.
    //private List<String> roles
    
    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }
    
}
