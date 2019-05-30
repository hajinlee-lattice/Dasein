package com.latticeengines.pls.service.impl;

import java.sql.Timestamp;

import javax.servlet.http.HttpServletRequest;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStatus;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStep;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.entitymanager.TenantDeploymentEntityMgr;
import com.latticeengines.pls.service.TenantDeploymentService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.exposed.util.SecurityUtils;

@Component("tenantDeploymentService")
public class TenantDeploymentServiceImpl implements TenantDeploymentService {

    @Autowired
    private SessionService sessionService;

    @Autowired
    private UserService userService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TenantDeploymentEntityMgr tenantDeploymentEntityMgr;


    @Override
    public TenantDeployment createTenantDeployment(String tenantId, HttpServletRequest request) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[]{tenantId});
        }

        TenantDeployment tenantDeployment = new TenantDeployment();
        tenantDeployment.setTenantId(tenant.getPid());
        tenantDeployment.setStep(TenantDeploymentStep.ENTER_CREDENTIALS);
        tenantDeployment.setStatus(TenantDeploymentStatus.NEW);
        User user = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        if (user == null) {
            throw new LedpException(LedpCode.LEDP_18221);
        }
        tenantDeployment.setCreatedBy(user.getEmail());
        DateTime dateTime = new DateTime(DateTimeZone.UTC);
        tenantDeployment.setCreateTime(new Timestamp(dateTime.getMillis()));
        tenantDeploymentEntityMgr.create(tenantDeployment);
        return tenantDeployment;
    }

    @Override
    public TenantDeployment getTenantDeployment(String tenantId) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[]{tenantId});
        }

        return tenantDeploymentEntityMgr.findByTenantId(tenant.getPid());
    }

    @Override
    public void updateTenantDeployment(TenantDeployment tenantDeployment) {
        Long tenantId = tenantDeployment.getTenantId();
        TenantDeployment deploymentFromDb = tenantDeploymentEntityMgr.findByTenantId(tenantId);
        if (deploymentFromDb == null) {
            throw new LedpException(LedpCode.LEDP_18063, new String[]{tenantId.toString()});
        }

        deploymentFromDb.setStep(tenantDeployment.getStep());
        deploymentFromDb.setStatus(tenantDeployment.getStatus());
        deploymentFromDb.setCurrentLaunchId(tenantDeployment.getCurrentLaunchId());
        deploymentFromDb.setModelId(tenantDeployment.getModelId());
        deploymentFromDb.setMessage(tenantDeployment.getMessage());
        tenantDeploymentEntityMgr.update(deploymentFromDb);
    }

    @Override
    public boolean isDeploymentCompleted(TenantDeployment tenantDeployment) {
        if (tenantDeployment.getStep() == TenantDeploymentStep.VALIDATE_METADATA
                && tenantDeployment.getStatus() == TenantDeploymentStatus.SUCCESS) {
            return true;
        } else {
            return false;
        }
    }

    public boolean deleteTenantDeployment(String tenantId) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[]{tenantId});
        }

        return tenantDeploymentEntityMgr.deleteByTenantId(tenant.getPid());
    }
}
