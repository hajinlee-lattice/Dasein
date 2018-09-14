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

import javax.servlet.http.HttpServletRequest;

public class JwtManagerTestNG extends SecurityFunctionalTestNGBase {

    @Autowired
    private JwtManager jwtManager;

    @Test(groups = "functional")
    public void generateJwtTokenWithRedirectURL() throws Exception {

        GlobalAuthUser user = new GlobalAuthUser();
        user.setFirstName("Tim");
        user.setLastName("Gates");
        user.setEmail("timg@microsoft.com");
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(ZendeskJwtHandler.ZENDESK_URL_QUERY_RETURN_TO_KEY, "https://lattice-engineshelp.zendesk.com");
        parameters.put(JwtManager.SOURCE_REF_KEY, ZendeskJwtHandler.HANDLER_NAME);
        JwtRequestParameters reqParameters = new JwtRequestParameters();
        reqParameters.setRequestParameters(parameters);
        String url = null;
        try {
            url = jwtManager.handleJwtRequest(null, user, reqParameters, true);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        Assert.assertNotNull(url);
        Assert.assertTrue(url.contains(ZendeskJwtHandler.ZENDESK_URL_QUERY_RETURN_TO_KEY));
        Assert.assertTrue(url.contains(ZendeskJwtHandler.ZENDESK_URL_QUERY_JWT_TOKEN_KEY));
        System.out.println("token url : " + url);
    }
}
