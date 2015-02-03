package com.latticeengines.pls.security;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;

@Component("globalAuthProvider")
public class GlobalAuthProvider implements AuthenticationProvider {
    
    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        Ticket ticket = (Ticket) authentication.getCredentials();
        
        try {
            Session session = globalSessionManagementService.retrieve(ticket);
            List<GrantedRight> rights = new ArrayList<>();
            
            for (String right : session.getRights()) {
                rights.add(GrantedRight.getGrantedRight(right));
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

    public GlobalSessionManagementService getGlobalSessionManagementService() {
        return globalSessionManagementService;
    }

    public void setGlobalSessionManagementService(GlobalSessionManagementService globalSessionManagementService) {
        this.globalSessionManagementService = globalSessionManagementService;
    }

}
