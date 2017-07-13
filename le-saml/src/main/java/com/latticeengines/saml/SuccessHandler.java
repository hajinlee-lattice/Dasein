package com.latticeengines.saml;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.saml.SAMLCredential;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;

import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.saml.util.SAMLUtils;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;

public class SuccessHandler extends SimpleUrlAuthenticationSuccessHandler {
    public static final Logger log = LoggerFactory.getLogger(SuccessHandler.class);

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
            Authentication authentication) throws ServletException, IOException {
        log.info(String.format("SAML Authentication successful for user %s", authentication.getName()));
        super.onAuthenticationSuccess(request, response, authentication);
    }

    @Override
    protected String determineTargetUrl(HttpServletRequest request, HttpServletResponse response) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String email = authentication.getName();
        Ticket ticket = globalAuthenticationService.externallyAuthenticated(email);
        SAMLCredential credential = (SAMLCredential) authentication.getCredentials();
        if (credential == null) {
            throw new RuntimeException(String.format("Credentials not found for user %s", email));
        }

        return String.format("%s?ticket_id=%s&tenant_id=%s", getDefaultTargetUrl(), ticket.getData(),
                SAMLUtils.getTenantIdFromLocalEntityId(credential.getLocalEntityID()));
    }
}
