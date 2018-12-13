package com.latticeengines.ulysses.web;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.provider.OAuth2Authentication;

import com.latticeengines.db.exposed.util.MultiTenantContextStrategy;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;

public class OAuth2MultiTenantContextStrategy implements MultiTenantContextStrategy {

    @Override
    public Tenant getTenant() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof OAuth2Authentication) {
            Tenant tenant = new Tenant();
            tenant.setId(CustomerSpace.parse(auth.getPrincipal().toString()).toString());
            tenant.setName(auth.getPrincipal().toString());
            return tenant;
        }
        return null;
    }

    @Override
    public Session getSession() {
        return null;
    }

    @Override
    public void setTenant(Tenant tenant) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void clear() {
        SecurityContextHolder.getContext().setAuthentication(null);
    }
}
