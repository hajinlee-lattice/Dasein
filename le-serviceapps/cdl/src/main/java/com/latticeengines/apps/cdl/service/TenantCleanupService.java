package com.latticeengines.apps.cdl.service;

public interface TenantCleanupService {

    boolean removeTenantTables(String tenantId);
}
