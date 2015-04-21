package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.Map;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;

public abstract class LatticeComponentInstaller implements CustomerSpaceServiceInstaller {

    private String componentName;

    protected LatticeComponentInstaller(String componentName) {
        this.componentName = componentName;
    }

    // the true installation steps other than writing to Camille
    protected abstract void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir);

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion, Map<String, String> properties) {
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(properties);
        DocumentDirectory dir = sDir.getDocumentDirectory();
        dir.makePathsLocal();
        installCore(space, serviceName, dataVersion, dir);
        return dir;
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
        Path metadataPath = PathBuilder.buildServiceConfigSchemaPath(podId, serviceName);
        return camille.getDirectory(metadataPath);
    }

    public DocumentDirectory getDefaultConfiguration() { return getDefaultConfiguration(componentName); }

    public DocumentDirectory getConfigurationSchema() { return getConfigurationSchema(componentName); }

    public SerializableDocumentDirectory getSerializableDefaultConfig() {
        DocumentDirectory configDir = getDefaultConfiguration();
        DocumentDirectory metaDir = getConfigurationSchema();
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadata(metaDir);
        return sDir;
    }
}
