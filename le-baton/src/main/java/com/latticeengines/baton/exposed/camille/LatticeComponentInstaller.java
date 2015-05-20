package com.latticeengines.baton.exposed.camille;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.message.BasicNameValuePair;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory.Node;
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

    public Document getDocument(DocumentDirectory configDir, String field){
        Node node = configDir.get("/" + field);
        return node != null ? node.getDocument() : null;
    }

    public String getData(DocumentDirectory configDir, String field) {
        Document doc = getDocument(configDir, field);
        return doc != null ? doc.getData() : null;
    }

    public Node getChild(DocumentDirectory configDir, String field, String childName){
        Node node = configDir.get("/" + field);
        return node.getChild(childName);
    }
 
    public List<Node> getChildren(DocumentDirectory configDir, String field){
        Node node = configDir.get("/" + field);
        return node.getChildren();
    }

    public List<BasicNameValuePair> getHeaders(){
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("MagicAuthentication", "Security through obscurity!"));
        headers.add(new BasicNameValuePair("Content-Type", "application/json"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));
        return headers;
    }
}
