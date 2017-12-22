package com.latticeengines.saml;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;

public class FailureHandler implements AuthenticationFailureHandler {
    public static final Logger log = LoggerFactory.getLogger(FailureHandler.class);

    @Override
    public void onAuthenticationFailure(HttpServletRequest request, HttpServletResponse response,
            AuthenticationException exception) throws IOException, ServletException {
        log.error(String.format("Failed to authenticate: %s", exception));

        LoginValidationResponse resp = new LoginValidationResponse();
        resp.setValidated(false);
        resp.setAuthenticationException(exception);

        ServletOutputStream os = response.getOutputStream();
        JsonUtils.serialize(resp, os);
        os.flush();

        response.setStatus(HttpStatus.SC_UNAUTHORIZED);
    }
}
