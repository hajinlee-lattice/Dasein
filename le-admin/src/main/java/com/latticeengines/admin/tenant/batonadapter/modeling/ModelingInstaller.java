package com.latticeengines.admin.tenant.batonadapter.modeling;

import com.latticeengines.baton.exposed.service.BatonService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.encryption.exposed.service.DataEncryptionService;

@Component
public class ModelingInstaller extends LatticeComponentInstaller {
    private static final Logger log = LoggerFactory.getLogger(ModelingInstaller.class);

    private DataEncryptionService dataEncryptionService;

    private BatonService batonService;

    public ModelingInstaller() {
        super(ModelingComponent.componentName);
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        boolean encrypt = batonService.isEnabled(space, LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION);

        log.info(String.format("Data encryption is set to %s for customer %s", encrypt, space));
        if (encrypt) {
            dataEncryptionService.encrypt(space);
        }
        return configDir;
    }

    public void setDataEncryptionService(DataEncryptionService dataEncryptionService) {
        this.dataEncryptionService = dataEncryptionService;
    }

    public void setBatonService(BatonService batonService) {
        this.batonService = batonService;
    }
}
