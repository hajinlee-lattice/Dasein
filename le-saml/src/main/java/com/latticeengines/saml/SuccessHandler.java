package com.latticeengines.saml;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;

public class SuccessHandler implements AuthenticationSuccessHandler {

    public static final Logger log = LoggerFactory.getLogger(SuccessHandler.class);

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
            Authentication authentication) throws ServletException, IOException {
        log.info(String.format("SAML Authentication successful for user %s", authentication.getName()));

        LoginValidationResponse resp = new LoginValidationResponse();

        String email = authentication.getName();

        log.info("request.getPathInfo() = " + request.getServletPath());
        log.info("email = " + email);

        String tenantId = request.getServletPath()
                .substring(request.getServletPath().lastIndexOf("/alias/") + "/alias/".length());
        log.info("tenantId = " + tenantId);

        ServletOutputStream os = response.getOutputStream();
        resp.setValidated(true);
        resp.setUserId(email);
        JsonUtils.serialize(resp, os);
        os.flush();

        response.setStatus(HttpStatus.SC_OK);
    }

}
