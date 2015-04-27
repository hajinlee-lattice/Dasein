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

    // dryrun flag to turn on/off true installation steps, for functional tests.
    private boolean dryrun = false;

    protected LatticeComponentInstaller(String componentName) {
        this.componentName = componentName;
    }

    // the true installation steps other than writing to Camille
    protected abstract void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir);

    public void setDryrun(boolean dryrun) { this.dryrun = dryrun; }

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion, Map<String, String> properties) {
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(properties);
        DocumentDirectory dir = sDir.getDocumentDirectory();
        dir.makePathsLocal();

        if (!dryrun) { installCore(space, serviceName, dataVersion, dir); }

        CustomerSpaceServiceBootstrapManager.reset(serviceName, space);
        return dir;
    }

    public DocumentDirectory getDefaultConfiguration() {
        Camille camille = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();
        Path defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(podId, componentName);
        return camille.getDirectory(defaultConfigPath);
    }

    public DocumentDirectory getConfigurationSchema() {
        Camille camille = CamilleEnvironment.getCamille();
        String podId = CamilleEnvironment.getPodId();
        Path metadataPath = PathBuilder.buildServiceConfigSchemaPath(podId, componentName);
        return camille.getDirectory(metadataPath);
    }

    public SerializableDocumentDirectory getSerializableDefaultConfig() {
        DocumentDirectory configDir = getDefaultConfiguration();
        DocumentDirectory metaDir = getConfigurationSchema();
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadata(metaDir);
        return sDir;
    }
}
