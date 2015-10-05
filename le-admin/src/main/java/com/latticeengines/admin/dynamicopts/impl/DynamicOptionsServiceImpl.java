package com.latticeengines.admin.dynamicopts.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.dynamicopts.MutableOptionsProvider;
import com.latticeengines.admin.dynamicopts.OptionsProvider;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

@Component
public class DynamicOptionsServiceImpl implements DynamicOptionsService {

    private static final Map<Path, OptionsProvider> optionMap = new ConcurrentHashMap<>();
    private static final String VDB_SERVER_PATH = "/VisiDBServers";

    @Value("${admin.mount.rootpath}")
    private String mountRoot;

    @Autowired
    private DataStoreProvider dataStoreProvider;

    @Autowired
    private TemplateProvider templateProvider;

    @PostConstruct
    private void registerProviders() {
        // CRM Topologies
        OptionsProvider topologyProvider = new EnumOptionsProvider(CRMTopology.class);

        // Lattice Products
        OptionsProvider productProvider = new EnumOptionsProvider(LatticeProduct.class);

        // register providers
        Path zkPath = new Path(VisiDBDLComponent.componentName, "DL", "DataStore");
        register(zkPath, dataStoreProvider);

        zkPath = new Path(VisiDBDLComponent.componentName, "TemplateVersion");
        register(zkPath, templateProvider);

        zkPath = new Path(LatticeComponent.spaceConfigNode, "Topology");
        register(zkPath, topologyProvider);

        zkPath = new Path(LatticeComponent.spaceConfigNode, "Product");
        register(zkPath, productProvider);
    }

    private void register(Path path, OptionsProvider provider) { optionMap.put(path, provider); }

    @Override
    public SelectableConfigurationDocument bind(SelectableConfigurationDocument doc) {
        String component = doc.getComponent();
        Path rootPath = new Path("/" + component);
        for (SelectableConfigurationField field: doc.getNodes()) {
            Path fullPath = rootPath.append(new Path(field.getNode()));
            if (optionMap.containsKey(fullPath)) {
                OptionsProvider provider = optionMap.get(fullPath);
                field.setOptions(provider.getOptions());
            }
        }
        return doc;
    }

    @Override
    public SerializableDocumentDirectory bind(SerializableDocumentDirectory sDir) {
        for (SerializableDocumentDirectory.Node node: sDir) {
            Path fullPath = new Path(sDir.getRootPath()).append(node.path);
            if (optionMap.containsKey(fullPath)) {
                OptionsProvider provider = optionMap.get(fullPath);
                bindToNode(node, provider.getOptions());
            }
        }
        return sDir;
    }

    @Override
    public OptionsProvider getProvider(Path node) {
        if (optionMap.containsKey(node)) {
            return optionMap.get(node);
        } else {
            return new OptionsProvider.NullProvider();
        }
    }

    @Override
    public void updateMutableOptionsProviderSource(String serviceName, SelectableConfigurationField field) {
        OptionsProvider provider = getProvider(new Path("/" + serviceName + field.getNode()));
        if (provider instanceof MutableOptionsProvider) {
            MutableOptionsProvider mutableOptionsProvider = (MutableOptionsProvider) provider;
            mutableOptionsProvider.setOptions(field.getOptions());
        }
    }

    private void bindToNode(SerializableDocumentDirectory.Node node, List<String> options) {
        SerializableDocumentDirectory.Metadata metadata = node.getMetadata();
        if (metadata == null) metadata = new SerializableDocumentDirectory.Metadata();
        if (metadata.getType() == null || !metadata.getType().equals("options")) metadata.setType("options");
        metadata.setOptions(options);
        node.setMetadata(metadata);
    }
}
