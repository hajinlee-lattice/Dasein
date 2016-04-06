package com.latticeengines.security.exposed;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;

public class TicketAuthenticationToken extends AbstractAuthenticationToken {
    private static final long serialVersionUID = 7250561373440320046L;
    private Object principal;
    private Ticket ticket;
    private Session session;

    public TicketAuthenticationToken(Object principal, String token) {
        this(principal, new Ticket(token), new ArrayList<GrantedRight>());
    }

    public TicketAuthenticationToken(Object principal, Ticket ticket, Collection<? extends GrantedAuthority> authorities) {
        super(authorities);
        this.principal = principal;
        this.ticket = ticket;
    }

    @Override
    public Object getCredentials() {
        return ticket;
    }

    @Override
    public Object getPrincipal() {
        return principal;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

}
