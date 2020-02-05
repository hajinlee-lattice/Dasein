package com.latticeengines.security.exposed.jwt;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.pls.JwtReplyParameters;
import com.latticeengines.domain.exposed.pls.JwtRequestParameters;
import com.latticeengines.security.exposed.jwt.handler.impl.ZendeskJwtHandler;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
public class JwtManagerTestNG extends SecurityFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(JwtManagerTestNG.class);

    @Inject
    private JwtManager jwtManager;

    @Test(groups = "functional")
    public void generateJwtTokenWithFunction() {
        GlobalAuthUser user = new GlobalAuthUser();
        user.setFirstName("Tim");
        user.setLastName("Gates");
        user.setEmail("timg@lattice-engines.com");
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(ZendeskJwtHandler.ZENDESK_URL_QUERY_RETURN_TO_KEY, "https://lattice-engines.zendesk.com");
        parameters.put(JwtManager.SOURCE_REF_KEY, ZendeskJwtHandler.HANDLER_NAME);
        JwtRequestParameters reqParameters = new JwtRequestParameters();
        reqParameters.setRequestParameters(parameters);
        JwtReplyParameters reply = null;
        try {
            reply = jwtManager.handleJwtRequest(user, reqParameters);
        } catch (Exception e) {
            log.error("Failed to handle Jwt request", e);
        }
        Assert.assertNotNull(reply);
        Assert.assertNotNull(reply.getType());
        Assert.assertNotNull(reply.getUrl());
        System.out.println("token function : " + reply.getType());
        System.out.println("token url : " + reply.getUrl());
    }
}
