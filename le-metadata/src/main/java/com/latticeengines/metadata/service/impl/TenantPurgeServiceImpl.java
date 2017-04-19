package com.latticeengines.metadata.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.metadata.service.TenantPurgeService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("tenantPurgeService")
public class TenantPurgeServiceImpl implements TenantPurgeService {

    @Autowired
    private SegmentService segmentService;

    @Autowired
    private TenantService tenantService;

    @Override
    public void purge(Tenant tenant) {
        tenant = tenantService.findByTenantId(tenant.getId());
        MultiTenantContext.setTenant(tenant);
        segmentService.deleteAllSegments(tenant.getId());
    }
}
