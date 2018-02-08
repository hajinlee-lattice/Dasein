/*
* Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
*
*     http://aws.amazon.com/apache2.0/
*
* or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/
package com.latticeengines.lambda.authorizer.handler;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpStatusCodeException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.lambda.authorizer.domain.AuthPolicy;
import com.latticeengines.lambda.authorizer.domain.ErrorMessage;
import com.latticeengines.lambda.authorizer.domain.GatewayPolicyConfiguration;
import com.latticeengines.lambda.authorizer.domain.TokenAuthorizerContext;
import com.latticeengines.lambda.authorizer.integ.UlyssesProvider;

/**
 * Handles IO for a Java Lambda function as a custom authorizer for API Gateway
 *
 */
@Component
public class APIGatewayAuthorizerHandler implements RequestHandler<TokenAuthorizerContext, AuthPolicy> {

    private static Logger LOG = Logger.getLogger(APIGatewayAuthorizerHandler.class);
    
    @Autowired
    private UlyssesProvider ulyssesProvider;
    
    @Override
    public AuthPolicy handleRequest(TokenAuthorizerContext input, Context context) {
        
        // validate the incoming token
        // and produce the principal user identifier associated with the token
        GatewayPolicyConfiguration policyConfig = null;
        String principalId = null;
        String clientApiKey = null;
        AuthPolicy.PolicyDocument policyDocument = null;
        try {
            policyConfig = ulyssesProvider.getGatewayPolicyConfig(input);
            principalId = policyConfig.getPrincipal() != null ? policyConfig.getPrincipal() : "UnKnown";
            clientApiKey = policyConfig.getApiKey();
            
            // if the token is valid, a policy should be generated which will allow or deny
            // access to the client
            policyDocument = generatePolicyDocument(input, true);
        } catch (Exception e) {
            handleException(e);
            policyDocument = generatePolicyDocument(input, false);
        }
        
        return new AuthPolicy(principalId, policyDocument, clientApiKey);
    }

    protected AuthPolicy.PolicyDocument generatePolicyDocument(TokenAuthorizerContext input, boolean allowPolicy) {
        String methodArn = input.getMethodArn();
        String[] arnPartials = methodArn.split(":");
        String region = arnPartials[3];
        String awsAccountId = arnPartials[4];
        String[] apiGatewayArnPartials = arnPartials[5].split("/");
        String restApiId = apiGatewayArnPartials[0];
        String stage = apiGatewayArnPartials[1];
        String httpMethod = apiGatewayArnPartials[2];
        String resource = ""; // root resource
        if (apiGatewayArnPartials.length == 4) {
            resource = apiGatewayArnPartials[3];
        }

        // this function must generate a policy that is associated with the recognized principal user identifier.
        // the example policy below denies access to all resources in the RestApi
        AuthPolicy.PolicyDocument policyDocument = allowPolicy
                ? AuthPolicy.PolicyDocument.getAllowAllPolicy(region, awsAccountId, restApiId, stage)
                : AuthPolicy.PolicyDocument.getDenyAllPolicy(region, awsAccountId, restApiId, stage);
        return policyDocument;
    }

    protected void handleException(Exception e) {
        LOG.error("Could not authorize the Request: " + e.getMessage());
        /*
        ErrorMessage errorMessage = null;
        if (e instanceof HttpStatusCodeException) {
            HttpStatus httpStatus = ((HttpStatusCodeException)e).getStatusCode();
            errorMessage = new ErrorMessage(httpStatus.value(), httpStatus.getReasonPhrase(), e.getMessage());
        }
        if (errorMessage == null) {
            errorMessage = new ErrorMessage(HttpStatus.UNAUTHORIZED.value(), HttpStatus.UNAUTHORIZED.getReasonPhrase(), e.getMessage());
        }
        throw new RuntimeException(getErrorMessageString(errorMessage));
        */
    }

    /*
    protected String getErrorMessageString(ErrorMessage errorMessage) {
        if(errorMessage == null) {
            return null;
        }
        try {
            return new ObjectMapper().writeValueAsString(errorMessage);
        } catch (JsonProcessingException e) {
            LOG.error("Error: " + e.getMessage());
            return "Could not parse ErrorMessage";
        }
    }
    */
}
