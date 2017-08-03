package com.latticeengines.security.provider;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.authentication.InternalAuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;

public abstract class AbstractAuthenticationTokenFilter extends AbstractAuthenticationProcessingFilter {

    protected AbstractAuthenticationTokenFilter() {
        super("/token_filter");
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) req;
        HttpServletResponse response = (HttpServletResponse) res;
        Authentication authResult;
        try {
            authResult = this.attemptAuthentication(request, response);
            if(authResult == null) {
                return;
            }
        } catch (InternalAuthenticationServiceException var8) {
            this.logger.error("An internal error occurred while trying to authenticate the user.", var8);
            this.unsuccessfulAuthentication(request, response, var8);
            return;
        } catch (AuthenticationException var9) {
            this.unsuccessfulAuthentication(request, response, var9);
            return;
        }
        this.successfulAuthentication(request, response, chain, authResult);
        chain.doFilter(request, response);
    }

}
