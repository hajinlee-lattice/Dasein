package com.latticeengines.admin.tenant.batonadapter.modeling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

@Component
public class ModelingInstaller extends LatticeComponentInstaller {
    private static final Logger log = LoggerFactory.getLogger(ModelingInstaller.class);

    private BatonService batonService;

    public ModelingInstaller() {
        super(ModelingComponent.componentName);
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
            int dataVersion, DocumentDirectory configDir) {
        try {
            boolean encrypt = batonService.isEnabled(space, LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION);

            if (encrypt) {
                log.info("Won't encrypt HDFS anymore");
                batonService.setFeatureFlag(space, LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION, false);
            }
        } catch (Exception e) {
            log.warn("Error when resetting HDFS encryption for " + space, e);
        }
        return configDir;
    }

    public void setBatonService(BatonService batonService) {
        this.batonService = batonService;
    }
}
