package com.latticeengines.security.exposed.service.impl;

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

@Component("tenantService")
public class TenantServiceImpl implements TenantService {
    private static final Log log = LogFactory.getLog(TenantServiceImpl.class);

    @Autowired
    public GlobalTenantManagementService globalTenantManagementService;

    @Autowired
    public TenantEntityMgr tenantEntityMgr;

    @Autowired
    private UserService userService;

    @Override
    public void registerTenant(Tenant tenant) {
        try {
            globalTenantManagementService.registerTenant(tenant);
        } catch (LedpException e) {
            log.warn("Error registering tenant with GA.", e);
        }
        tenantEntityMgr.create(tenant);
    }

    @Override
    public void updateTenant(Tenant tenant) {
        Tenant oldTenant = tenantEntityMgr.findByTenantId(tenant.getId());
        oldTenant.setName(tenant.getName());
        if (tenant.getRegisteredTime() == null) {
            oldTenant.setRegisteredTime(new Date().getTime());
        } else {
            oldTenant.setRegisteredTime(tenant.getRegisteredTime());
        }
        try {
            globalTenantManagementService.registerTenant(tenant);
        } catch (LedpException e) {
            log.warn("Tenant already exist in GA.", e);
        }
        tenantEntityMgr.update(oldTenant);
    }

    @Override
    public void discardTenant(Tenant tenant) {
        try {
            tenantEntityMgr.delete(tenant);
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains("null entity")) {
                throw e;
            }
        }
        try {
            for (User user : userService.getUsers(tenant.getId())){
                userService.deleteUser(tenant.getId(), user.getUsername());
            }
            globalTenantManagementService.discardTenant(tenant);
        } catch (LedpException e) {
            log.warn("Error discarding tenant with GA.", e);
        }
    }

    @Override
    public List<Tenant> getAllTenants() {
        return tenantEntityMgr.findAll();
    }

    @Override
    public boolean hasTenantId(String tenantId) {
        return tenantEntityMgr.findByTenantId(tenantId) != null;
    }

    @Override
    public Tenant findByTenantId(String tenantId) {
        return tenantEntityMgr.findByTenantId(tenantId);
    }

    @Override
    public Tenant findByTenantName(String tenantName) { return tenantEntityMgr.findByTenantName(tenantName); }

}
