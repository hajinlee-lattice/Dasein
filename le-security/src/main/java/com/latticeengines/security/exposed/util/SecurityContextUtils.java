package com.latticeengines.security.exposed.util;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TenantToken;
import com.latticeengines.security.exposed.TicketAuthenticationToken;

public class SecurityContextUtils {
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

    public static void setTenant(Tenant tenant) {
        Authentication auth = new TenantToken(tenant);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }
}
