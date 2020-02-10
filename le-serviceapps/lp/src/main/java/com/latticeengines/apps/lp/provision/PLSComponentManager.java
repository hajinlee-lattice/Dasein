package com.latticeengines.apps.lp.provision;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.component.InstallDocument;

public interface PLSComponentManager {

    void provisionTenant(CustomerSpace space, InstallDocument installDocument);

    void discardTenant(String tenantId);

    void provisionTenant(CustomerSpace space, DocumentDirectory configDir);

}
