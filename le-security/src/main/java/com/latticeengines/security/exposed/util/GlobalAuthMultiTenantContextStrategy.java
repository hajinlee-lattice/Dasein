package com.latticeengines.security.exposed.util;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContextStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.TenantToken;
import com.latticeengines.security.exposed.TicketAuthenticationToken;

public class GlobalAuthMultiTenantContextStrategy implements MultiTenantContextStrategy {

    @Override
    public Tenant getTenant() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        Tenant tenant;
        String authSource;
        if (auth instanceof TicketAuthenticationToken) {
            TicketAuthenticationToken token = (TicketAuthenticationToken) auth;
            tenant = token.getSession().getTenant();
            authSource = "TicketAuthenticationToken";
        } else if (auth instanceof TenantToken) {
            tenant = ((TenantToken) auth).getTenant();
            authSource = "TenantToken";
        } else {
            return null;
        }

        if (tenant != null && tenant.getPid() == null) {
            throw new IllegalStateException(
                    "Should not have a " + authSource + " with null PID in SecurityContextHolder: " + JsonUtils.serialize(auth));
        }

        return tenant;
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
    public User getUser() {
        Session session = getSession();
        if (session == null) {
            throw new LedpException(LedpCode.LEDP_18221);
        }
        User user = new User();
        user.setEmail(session.getEmailAddress());
        user.setAccessLevel(session.getAccessLevel());
        return user;
    }

    @Override
    public void setTenant(Tenant tenant) {
        Authentication auth = new TenantToken(tenant);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @Override
    public void clear() {
        SecurityContextHolder.getContext().setAuthentication(null);
    }
}
