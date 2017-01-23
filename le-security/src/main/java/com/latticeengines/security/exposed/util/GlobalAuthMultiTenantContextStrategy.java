package com.latticeengines.security.exposed.util;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TenantToken;
import com.latticeengines.security.exposed.TicketAuthenticationToken;

public class GlobalAuthMultiTenantContextStrategy extends MultiTenantContextStrategy {
    @Override
    public Tenant getTenant() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof TicketAuthenticationToken) {
            TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
            return token.getSession().getTenant();
        } else if (auth instanceof TenantToken) {
            return ((TenantToken) auth).getTenant();
        } else {
            return null;
        }
    }

    @Override
    public Session getSession() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof TicketAuthenticationToken) {
            TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
            if (token.getSession() != null && token.getSession().getEmailAddress() != null) {
                return token.getSession();
            }
        }
        return null;
    }

    @Override
    public void setTenant(Tenant tenant) {
        Authentication auth = new TenantToken(tenant);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}
