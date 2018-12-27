package com.latticeengines.admin.tenant.batonadapter.modeling;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;

public class ModelingDestroyer implements CustomerSpaceServiceDestroyer {

    private ModelingComponentManager modelingComponentManager;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        modelingComponentManager.cleanupHdfs(space);
        return true;
    }

    public void setModelingComponentManager(ModelingComponentManager modelingComponentManager) {
        this.modelingComponentManager = modelingComponentManager;
    }
}
