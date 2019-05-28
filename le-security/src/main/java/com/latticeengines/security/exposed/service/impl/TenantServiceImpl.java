package com.latticeengines.security.exposed.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

@Component("tenantService")
public class TenantServiceImpl implements TenantService {
    private static final Logger log = LoggerFactory.getLogger(TenantServiceImpl.class);

    @Autowired
    public GlobalTenantManagementService globalTenantManagementService;

    @Autowired
    public TenantEntityMgr tenantEntityMgr;

    @Autowired
    private GlobalUserManagementService gaUserManagementService;

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
    public void registerTenant(Tenant tenant, String userName) {
        try {
            globalTenantManagementService.registerTenant(tenant, userName);
        } catch (LedpException e) {
            log.warn("Error registering tenant with GA.", e);
        }
        tenantEntityMgr.create(tenant);
    }

    @Override
    public void updateTenant(Tenant tenant) {
        Tenant oldTenant = tenantEntityMgr.findByTenantId(tenant.getId());
        oldTenant.setName(tenant.getName());
        oldTenant.setUiVersion(tenant.getUiVersion());
        oldTenant.setTenantType(tenant.getTenantType());
        oldTenant.setStatus(tenant.getStatus());
        oldTenant.setContract(tenant.getContract());
        if (tenant.getRegisteredTime() == null) {
            oldTenant.setRegisteredTime(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        } else {
            oldTenant.setRegisteredTime(tenant.getRegisteredTime());
        }
        oldTenant.setExpiredTime(tenant.getExpiredTime());
        if (!globalTenantManagementService.tenantExists(tenant)) {
            globalTenantManagementService.registerTenant(tenant);
        }
        tenantEntityMgr.update(oldTenant);
    }

    @Override
    public void updateTenantEmailFlag(String tenantId, boolean emailSent) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        if (tenant != null) {
            tenant.setEmailSent(emailSent);
            tenantEntityMgr.update(tenant);
        }
    }

    @Override
    public void discardTenant(Tenant tenant) {
        try {
            tenantEntityMgr.delete(tenant);
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains("null entity")) {
                throw e;
            } else {
                log.error(e.getMessage(), e);
            }
        }
        try {
            for (User user : userService.getUsers(tenant.getId())) {
                userService.deleteUser(tenant.getId(), user.getUsername());
                // check if the user has any tenant right, if not, deactivate zendesk user
                gaUserManagementService.checkRedundant(user.getUsername());
            }
            globalTenantManagementService.discardTenant(tenant);
        } catch (LedpException e) {
            log.warn("Error discarding tenant with GA.");
        }
    }

    @Override
    public List<Tenant> getAllTenants() {
        return tenantEntityMgr.findAll();
    }

    @Override
    public List<Tenant> getTenantsByStatus(TenantStatus status) {
        return tenantEntityMgr.findAllByStatus(status);
    }

    @Override
    public List<Tenant> getTenantByType(TenantType type) {
        return tenantEntityMgr.findAllByType(type);
    }

    @Override
    public boolean getTenantEmailFlag(String tenantId) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        if (tenant != null && tenant.getEmailSent() != null) {
            return tenant.getEmailSent();
        }
        return false;
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
    public Tenant findByTenantName(String tenantName) {
        return tenantEntityMgr.findByTenantName(tenantName);
    }

    @Override
    public void setNotificationStateByTenantId(String tenantId, int status) {
        tenantEntityMgr.setNotificationStateByTenantId(tenantId, status);
    }

}
