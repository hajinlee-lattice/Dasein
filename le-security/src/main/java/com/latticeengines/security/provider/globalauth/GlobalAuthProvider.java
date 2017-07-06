package com.latticeengines.security.provider.globalauth;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.security.exposed.service.SessionService;

@Component("globalAuthProvider")
public class GlobalAuthProvider implements AuthenticationProvider {
    private static final Log log = LogFactory.getLog(GlobalAuthProvider.class);

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
            try {
                if (StringUtils.isNotEmpty(session.getAccessLevel())) {
                    rights.add(AccessLevel.valueOf(session.getAccessLevel()));
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Cannot understand access level " + session.getAccessLevel() + " in the session", e);
            }
            TicketAuthenticationToken token = new TicketAuthenticationToken( //
                    authentication.getPrincipal(), ticket, rights);
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
