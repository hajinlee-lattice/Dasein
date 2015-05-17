package com.latticeengines.admin.dynamicopts.impl;

import java.nio.file.FileSystems;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.dynamicopts.DynamicOptionsService;
import com.latticeengines.admin.dynamicopts.OptionsProvider;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.pls.PLSComponent;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

@Component
public class DynamicOptionsServiceImpl implements DynamicOptionsService {

    private static final Map<Path, OptionsProvider> optionMap = new ConcurrentHashMap<>();

    @Value("${admin.mount.rootpath}")
    private String mountRoot;

    @Value("${admin.mount.pls}")
    private String plsFolder;

    @PostConstruct
    private void registerProviders() {
        // CRM Topologies
        OptionsProvider topologyProvider = new EnumOptionsProvider(CRMTopology.class);
        register(new Path(LatticeComponent.spaceConfigNode, "Topology"), topologyProvider);

        // Lattice Products
        OptionsProvider productProvider = new EnumOptionsProvider(LatticeProduct.class);
        register(new Path(LatticeComponent.spaceConfigNode, "Product"), productProvider);

        // this is an example of using SubdirectoryOptionsProvider
        Path zkPath = new Path(PLSComponent.componentName, "Directory");
        if (!optionMap.containsKey(zkPath)) {
            // avoid duplicated instantiation: each SubdirectoryOptionsProvider has a long polling watcher thread
            java.nio.file.Path filePath = FileSystems.getDefault().getPath(mountRoot, plsFolder);
            OptionsProvider plsProvider = new SubdirectoryOptionsProvider(filePath);
            register(zkPath, plsProvider);
        }

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
        Iterator<SerializableDocumentDirectory.Node> iter = sDir.getDepthFirstIterator();
        while(iter.hasNext()) {
            SerializableDocumentDirectory.Node node = iter.next();
            Path fullPath = new Path(sDir.getRootPath()).append(node.path);
            if (optionMap.containsKey(fullPath)) {
                OptionsProvider provider = optionMap.get(fullPath);
                bindToNode(node, provider.getOptions());
            }
        }
        return sDir;
    }

    private void bindToNode(SerializableDocumentDirectory.Node node, List<String> options) {
        SerializableDocumentDirectory.Metadata metadata = node.getMetadata();
        if (metadata == null) metadata = new SerializableDocumentDirectory.Metadata();
        if (metadata.getType() == null || !metadata.getType().equals("options")) metadata.setType("options");
        metadata.setOptions(options);
        node.setMetadata(metadata);
    }
}
