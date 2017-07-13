package com.latticeengines.saml;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;

public class FailureHandler extends SimpleUrlAuthenticationFailureHandler {
    public static final Logger log = LoggerFactory.getLogger(FailureHandler.class);

    @Override
    public void onAuthenticationFailure(HttpServletRequest request, HttpServletResponse response,
            AuthenticationException exception) throws IOException, ServletException {
        log.error(String.format("Failed to authenticate: %s", exception));
        super.onAuthenticationFailure(request, response, exception);
    }
}
