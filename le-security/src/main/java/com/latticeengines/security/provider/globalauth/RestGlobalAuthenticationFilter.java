package com.latticeengines.security.provider.globalauth;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;

import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.service.SessionService;

public class RestGlobalAuthenticationFilter extends AbstractAuthenticationProcessingFilter {

    @Autowired
    private SessionService sessionService;

    public RestGlobalAuthenticationFilter() {
        super("/globalauth_security_check");
    }

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response)
            throws AuthenticationException, IOException, ServletException {

        String methodName = String.format("RestGlobalAuthenticationFilter.attemptAuthentication [%s]",
                request.getRequestURL());
        try (PerformanceTimer timer = new PerformanceTimer(methodName)) {
            String ticket = request.getHeader(Constants.AUTHORIZATION);
            detectSessionCacheDirtiness(request);

            if (ticket == null) {
                throw new BadCredentialsException("Unauthorized.");
            }
            TicketAuthenticationToken authRequest = new TicketAuthenticationToken(null, ticket);
            return this.getAuthenticationManager().authenticate(authRequest);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    protected boolean requiresAuthentication(HttpServletRequest request, HttpServletResponse response) {
        boolean retVal = false;
        String ticket = request.getHeader(Constants.AUTHORIZATION);
        if (ticket != null) {
            Authentication authResult = null;
            try {
                try {
                    authResult = attemptAuthentication(request, response);
                } catch (IOException | ServletException e) {
                    throw new AuthenticationServiceException(e.getMessage(), e);
                }
                if (authResult == null) {
                    retVal = false;
                }
            } catch (AuthenticationException failed) {
                try {
                    unsuccessfulAuthentication(request, response, failed);
                } catch (IOException e) {
                    retVal = false;
                } catch (ServletException e) {
                    retVal = false;
                }
                retVal = false;
            }
            try {
                successfulAuthentication(request, response, authResult);
            } catch (IOException e) {
                retVal = false;
            } catch (ServletException e) {
                retVal = false;
            }
            return false;
        } else {
            retVal = true;
        }
        return retVal;
    }

    private void detectSessionCacheDirtiness(HttpServletRequest request) {
        String tenantId = request.getHeader(Constants.TENANT_ID);
        String token = request.getHeader(Constants.AUTHORIZATION);
        if (tenantId != null) {
            sessionService.clearCacheIfNecessary(tenantId, token);
        }
    }
}
