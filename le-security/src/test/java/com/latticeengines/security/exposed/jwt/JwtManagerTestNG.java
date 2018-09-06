package com.latticeengines.security.exposed.jwt;

import org.testng.annotations.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.pls.JwtRequestParameters;
import com.latticeengines.security.exposed.jwt.JwtManager;
import com.latticeengines.security.exposed.jwt.handler.impl.ZendeskJwtHandler;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import java.util.HashMap;
import java.util.Map;

public class JwtManagerTestNG extends SecurityFunctionalTestNGBase {

    @Autowired
    private JwtManager jwtManager;

    @Test(groups = "functional")
    public void generateJwtTokenWithRedirectURL() throws Exception {

        GlobalAuthUser user = new GlobalAuthUser();
        user.setFirstName("Lattice");
        user.setLastName("Dev Group");
        user.setEmail("ldg@lattice-engines.com");
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(ZendeskJwtHandler.ZENDESK_URL_QUERY_RETURN_TO_KEY, "https://lattice-engineshelp.zendesk.com");
        parameters.put(JwtManager.SOURCE_REF_KEY, ZendeskJwtHandler.HANDLER_NAME);
        JwtRequestParameters reqParameters = new JwtRequestParameters();
        reqParameters.setRequestParameters(parameters);
        String url = null;
        String token = null;
        try {
            url = jwtManager.handleJwtRequest(user, reqParameters, true);
            token = jwtManager.handleJwtRequest(user, reqParameters, false);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        Assert.assertNotNull(url);
        Assert.assertNotNull(token);
        Assert.assertTrue(url.contains(ZendeskJwtHandler.ZENDESK_URL_QUERY_RETURN_TO_KEY));
        Assert.assertTrue(url.contains(ZendeskJwtHandler.ZENDESK_URL_QUERY_JWT_TOKEN_KEY));
        System.out.println("token url : " + url);
        System.out.println("token token : " + token);
    }
}
