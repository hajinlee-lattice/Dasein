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
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public abstract class LatticeComponentInstaller implements CustomerSpaceServiceInstaller {

    private String componentName;
    // dry run flag: when it is up, installer only write default configuration to camille, skip all other installation steps.
    private boolean dryrun = true;

    protected LatticeComponentInstaller(String componentName) {
        this.componentName = componentName;
    }

    // the true installation steps other than writing to Camille
    protected abstract void installCore(
            CustomerSpace space, String serviceName, int dataVersion,
            CustomerSpaceProperties spaceProps, DocumentDirectory configDir);

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion, Map<String, String> properties) {
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(properties);
        Map<String, String> residualProps = sDir.getOtherProperties();
        DocumentDirectory dir = sDir.getDocumentDirectory();
        dir.makePathsLocal();

        if (!dryrun) {
            // do the true installation
            installCore(space, serviceName, dataVersion, new CustomerSpaceProperties(), dir);
        }

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

    public void setDryrun(boolean dryrun) { this.dryrun = dryrun; }
}
