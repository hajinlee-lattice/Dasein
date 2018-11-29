package com.latticeengines.apps.lp.provision;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.component.InstallDocument;

public interface LPComponentManager {

    void provisionTenant(CustomerSpace space, InstallDocument installDocument);

    void discardTenant(String tenantId);

}
