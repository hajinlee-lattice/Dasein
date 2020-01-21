package com.latticeengines.saml;

import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.saml.LoginValidationResponse;
import com.latticeengines.saml.util.SAMLUtils;

public class FailureHandler implements AuthenticationFailureHandler {
    private static final Logger log = LoggerFactory.getLogger(FailureHandler.class);

    @Override
    public void onAuthenticationFailure(HttpServletRequest request, HttpServletResponse response,
            AuthenticationException exception) throws IOException {
        try {
            log.info(String.format("Failed to authenticate: %s", exception));
            String tenantId = SAMLUtils.getTenantFromAlias(request.getPathInfo());
            log.info(String.format("request.getPathInfo() = %s, tenantId = %s", request.getPathInfo(), tenantId));
        } catch (Exception ex) {
            log.info("Ignoring error during logging: ", ex);
        }

        LoginValidationResponse resp = new LoginValidationResponse();

        try (ServletOutputStream os = response.getOutputStream()) {
            response.setContentType(MediaType.APPLICATION_JSON);
            response.setStatus(HttpStatus.SC_OK);
            resp.setValidated(false);
            resp.setAuthenticationException(exception);
            JsonUtils.serialize(resp, os);
        }
    }
}
