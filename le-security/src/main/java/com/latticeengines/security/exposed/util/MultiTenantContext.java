package com.latticeengines.security.exposed.util;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TenantToken;
import com.latticeengines.security.exposed.TicketAuthenticationToken;

public class MultiTenantContext {
    public static Tenant getTenant() {
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

    public static boolean isContextSet() {
        return getTenant() != null;
    }

    public static CustomerSpace getCustomerSpace() {
        Tenant tenant = getTenant();
        if (tenant == null) {
            throw new RuntimeException("No tenant set in context");
        }
        return CustomerSpace.parse(tenant.getId());
    }

    public static Session getSession() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof TicketAuthenticationToken) {
            TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
            if (token.getSession() != null && token.getSession().getEmailAddress() != null) {
                return token.getSession();
            }
        }
        return null;
    }

    public static String getEmailAddress() {
        Session session = getSession();
        if (session == null) {
            return null;
        }
        return session.getEmailAddress();
    }

    public static void setTenant(Tenant tenant) {
        Authentication auth = new TenantToken(tenant);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}
