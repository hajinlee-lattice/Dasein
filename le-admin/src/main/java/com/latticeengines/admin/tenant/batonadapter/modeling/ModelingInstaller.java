package com.latticeengines.admin.tenant.batonadapter.modeling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.encryption.exposed.service.DataEncryptionService;

@Component
public class ModelingInstaller extends LatticeComponentInstaller {
    private static final Log log = LogFactory.getLog(ModelingInstaller.class);

    private DataEncryptionService dataEncryptionService;

    public ModelingInstaller() {
        super(ModelingComponent.componentName);
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        boolean encrypt = FeatureFlagClient.isEnabled(space, LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION.getName());

        log.info(String.format("Data encryption is set to %s for customer %s", encrypt, space));
        if (encrypt) {
            dataEncryptionService.encrypt(space);
        }
        return configDir;
    }

    public void setDataEncryptionService(DataEncryptionService dataEncryptionService) {
        this.dataEncryptionService = dataEncryptionService;
    }
}
