package com.latticeengines.proxy.exposed.admin;

import java.util.List;

public interface AdminProxy {

    void deleteTenant(String contractId, String tenantId);

    List<String> getAllTenantIds();
}
