package com.latticeengines.security.provider.globalauth;

import static com.latticeengines.security.provider.AbstractAuthenticationTokenFilter.addRolePrefix;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.service.SessionService;

@Component("globalAuthProvider")
public class GlobalAuthProvider implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(GlobalAuthProvider.class);

    @Autowired
    private SessionService sessionService;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        Ticket ticket = (Ticket) authentication.getCredentials();

        try {
            Session session = sessionService.retrieve(ticket);
            List<GrantedAuthority> rights = new ArrayList<>();

            for (String right : session.getRights()) {
                GrantedRight grantedRight = GrantedRight.getGrantedRight(right);

                if (grantedRight != null) {
                    rights.add(grantedRight);
                } else {
                    log.error(LedpException.buildMessageWithCode(LedpCode.LEDP_18019, new String[] { right }));
                }

            }
            List<GrantedAuthority> authoritiesWithPrefix = rights.stream() //
                    .map(auth -> new SimpleGrantedAuthority(addRolePrefix(auth.getAuthority()))) //
                    .collect(Collectors.toList());
            TicketAuthenticationToken token = new TicketAuthenticationToken( //
                    session.getEmailAddress(), ticket, authoritiesWithPrefix);
            token.setSession(session);
            token.setAuthenticated(true);

            return token;
        } catch (Exception e) {
            throw new BadCredentialsException(e.getMessage(), e);
        }

    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.isAssignableFrom(TicketAuthenticationToken.class);
    }

}
