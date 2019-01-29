package com.latticeengines.security.provider.globalauth;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.provider.AbstractAuthenticationTokenFilter;

public class RestGlobalAuthenticationFilter extends AbstractAuthenticationTokenFilter {

    @Inject
    private SessionService sessionService;

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response)
            throws AuthenticationException {

        String ticket = request.getHeader(Constants.AUTHORIZATION);
        detectSessionCacheDirtiness(request);
        if (ticket == null) {
            throw new BadCredentialsException("Unauthorized.");
        }
        TicketAuthenticationToken authRequest;
        try {
            authRequest = new TicketAuthenticationToken(null, ticket);
        } catch (IllegalArgumentException e) {
            throw new BadCredentialsException(e.getMessage());
        }
        return this.getAuthenticationManager().authenticate(authRequest);
    }

    @SuppressWarnings("deprecation")
    private void detectSessionCacheDirtiness(HttpServletRequest request) {
        String tenantId = request.getHeader(Constants.TENANT_ID);
        String token = request.getHeader(Constants.AUTHORIZATION);
        if (tenantId != null) {
            sessionService.clearCacheIfNecessary(tenantId, token);
        }
    }
}
