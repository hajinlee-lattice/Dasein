package com.latticeengines.admin.tenant.batonadapter.modeling;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceDestroyer;
import com.latticeengines.encryption.exposed.service.DataEncryptionService;

public class ModelingDestroyer implements CustomerSpaceServiceDestroyer {

    private DataEncryptionService dataEncryptionService;

    private ModelingComponentManager modelingComponentManager;

    @Override
    public boolean destroy(CustomerSpace space, String serviceName) {
        modelingComponentManager.cleanupHdfs(space);
        boolean encrypt = FeatureFlagClient.isEnabled(space, LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION.getName());
        if (encrypt) {
            dataEncryptionService.deleteKey(space);
        }
        return true;
    }

    public void setDataEncryptionService(DataEncryptionService dataEncryptionService) {
        this.dataEncryptionService = dataEncryptionService;
    }

    public void setModelingComponentManager(ModelingComponentManager modelingComponentManager) {
        this.modelingComponentManager = modelingComponentManager;
    }
}
