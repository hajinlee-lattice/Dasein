package com.latticeengines.saml.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;
import com.latticeengines.saml.service.IdentityProviderService;
import com.latticeengines.security.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("IdentityProviderService")
public class IdentityProviderServiceImpl implements IdentityProviderService {
    @Autowired
    private IdentityProviderEntityMgr identityProviderEntityMgr;

    @Autowired
    private GlobalAuthTenantEntityMgr globalAuthTenantEntityMgr;

    @Override
    public void create(IdentityProvider identityProvider) {
        Tenant tenant = getTenant();
        GlobalAuthTenant gatenant = globalAuthTenantEntityMgr.findByTenantId(tenant.getId());
        identityProvider.setGlobalAuthTenant(gatenant);
        identityProviderEntityMgr.create(identityProvider);
    }

    @Override
    public void delete(String entityId) {
        Tenant tenant = getTenant();
        IdentityProvider identityProvider = identityProviderEntityMgr.findByEntityId(entityId);
        if (!identityProvider.getGlobalAuthTenant().getId().equals(tenant.getId())) {
            throw new AccessDeniedException("Unauthorized");
        }

        identityProviderEntityMgr.delete(identityProvider);
    }

    @Override
    public IdentityProvider find(String entityId) {
        Tenant tenant = getTenant();
        IdentityProvider identityProvider = identityProviderEntityMgr.findByEntityId(entityId);
        if (!identityProvider.getGlobalAuthTenant().getId().equals(tenant.getId())) {
            throw new AccessDeniedException("Unauthorized");
        }

        return identityProvider;
    }

    @Override
    public List<IdentityProvider> findAll() {
        Tenant tenant = getTenant();
        GlobalAuthTenant gatenant = globalAuthTenantEntityMgr.findByTenantId(tenant.getId());
        return identityProviderEntityMgr.findByTenantId(gatenant.getId());
    }

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new RuntimeException("No tenant supplied in context");
        }
        return tenant;
    }
}
