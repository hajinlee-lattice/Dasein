package com.latticeengines.security.exposed;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

import com.latticeengines.domain.exposed.security.Tenant;

public class TenantToken implements Authentication {
    
    private static final long serialVersionUID = -1071669783309035334L;
    private Tenant tenant;
    
    public TenantToken(Tenant tenant) {
        setTenant(tenant);
    }

    @Override
    public String getName() {
        return "tenantToken-" + Thread.currentThread().getName();
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return new ArrayList<GrantedAuthority>();
    }

    @Override
    public Object getCredentials() {
        return null;
    }

    @Override
    public Object getDetails() {
        return null;
    }

    @Override
    public Object getPrincipal() {
        return null;
    }

    @Override
    public boolean isAuthenticated() {
        return false;
    }

    @Override
    public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {
    }

    public Tenant getTenant() {
        return tenant;
    }
    
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }
}
