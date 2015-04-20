package com.latticeengines.admin.tenant.batonadapter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;

public abstract class LatticeComponentInstaller implements CustomerSpaceServiceInstaller {

    private String componentName;

    protected LatticeComponentInstaller(String componentName) {
        this.componentName = componentName;
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
