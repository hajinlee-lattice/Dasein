package com.latticeengines.apps.dcp.provision;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface DCPComponentManager {

    void provisionTenant(CustomerSpace space, DocumentDirectory configDir);
    void discardTenant(String tenantId);

}
