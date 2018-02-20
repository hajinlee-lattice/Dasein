package com.latticeengines.apps.core.service;

import com.latticeengines.domain.exposed.serviceapps.core.BootstrapRequest;

public interface ApplicationTenantService {

    void bootstrap(BootstrapRequest request);

    void cleanup(String tenantId);

}
