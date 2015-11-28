package com.latticeengines.pls.service;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.pls.TenantDeployment;

public interface TenantDeploymentService {

    TenantDeployment createTenantDeployment(String tenantId, HttpServletRequest request);

    TenantDeployment getTenantDeployment(String tenantId);

    void updateTenantDeployment(TenantDeployment tenantDeployment);

    boolean isDeploymentCompleted(TenantDeployment tenantDeployment);

}
