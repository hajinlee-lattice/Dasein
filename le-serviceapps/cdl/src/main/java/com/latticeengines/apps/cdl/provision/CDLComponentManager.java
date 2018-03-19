package com.latticeengines.apps.cdl.provision;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public interface CDLComponentManager {

    void provisionTenant(CustomerSpace space, DocumentDirectory configDir);
    void discardTenant(String tenantId);

}
