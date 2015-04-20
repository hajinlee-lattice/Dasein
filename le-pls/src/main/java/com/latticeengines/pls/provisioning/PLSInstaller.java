package com.latticeengines.pls.provisioning;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;

public class PLSInstaller implements CustomerSpaceServiceInstaller {
    private static final Log LOGGER = LogFactory.getLog(PLSInstaller.class);

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion, Map<String, String> properties) {
        return null;
    }

    @Override
    public DocumentDirectory getDefaultConfiguration(String serviceName) {
        Camille camille = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();
        Path defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(podId, serviceName);
        return camille.getDirectory(defaultConfigPath);
    }

    @Override
    public DocumentDirectory getConfigurationSchema(String serviceName) {
        Camille camille = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();
        Path defaultConfigPath = PathBuilder.buildServiceConfigSchemaPath(podId, serviceName);
        return camille.getDirectory(defaultConfigPath);
    }


}
