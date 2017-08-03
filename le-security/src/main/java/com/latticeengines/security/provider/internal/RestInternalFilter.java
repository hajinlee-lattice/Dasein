package com.latticeengines.security.provider.internal;

import java.io.IOException;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.provider.AbstractAuthenticationTokenFilter;

public class RestInternalFilter extends AbstractAuthenticationTokenFilter {

    private static final Authentication INTERNAL_TOKEN = //
            new AnonymousAuthenticationToken("InternalKey", "InternalPrincipal",
                    Collections.singleton(new SimpleGrantedAuthority("ROLE_INTERNAL_USER")));

    private static final Authentication EXTERNAL_TOKEN = //
            new AnonymousAuthenticationToken("ExternalKey", "ExternalPrincipal",
                    Collections.singleton(new SimpleGrantedAuthority("ROLE_EXTERNAL_USER")));

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response)
            throws AuthenticationException, IOException, ServletException {
        String ticket = request.getHeader(Constants.INTERNAL_SERVICE_HEADERNAME);
        if (Constants.INTERNAL_SERVICE_HEADERVALUE.equals(ticket)) {
            return INTERNAL_TOKEN;
        } else {
            return EXTERNAL_TOKEN;
        }
    }
}
