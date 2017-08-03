package com.latticeengines.security.provider.globalauth;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.provider.AbstractAuthenticationTokenFilter;

public class RestGlobalAuthenticationFilter extends AbstractAuthenticationTokenFilter {

    @Autowired
    private SessionService sessionService;

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response)
            throws AuthenticationException, IOException, ServletException {

        String methodName = String.format("RestGlobalAuthenticationFilter.attemptAuthentication [%s]",
                request.getRequestURI());
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
    private void detectSessionCacheDirtiness(HttpServletRequest request) {
        String tenantId = request.getHeader(Constants.TENANT_ID);
        String token = request.getHeader(Constants.AUTHORIZATION);
        if (tenantId != null) {
            sessionService.clearCacheIfNecessary(tenantId, token);
        }
    }
}
