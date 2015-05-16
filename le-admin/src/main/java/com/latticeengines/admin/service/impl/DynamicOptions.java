package com.latticeengines.admin.service.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.stereotype.Component;

import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

@Component
public class DynamicOptions {

    private Map<Path, List<String>> optionMap = new ConcurrentHashMap<>();
    private Map<Path, ReadWriteLock> locks = new ConcurrentHashMap<>();
    private static final String podId = CamilleEnvironment.getPodId();

    public DynamicOptions() {
        // CRM Topologies
        Path spaceConfigPath = PathBuilder.buildServiceConfigSchemaPath(
                CamilleEnvironment.getPodId(), LatticeComponent.spaceConfigNode);
        setOptions(spaceConfigPath.append(new Path("/Topology")), CRMTopology.getNames());

        // Lattice Products
        spaceConfigPath = PathBuilder.buildServiceConfigSchemaPath(
                CamilleEnvironment.getPodId(), LatticeComponent.spaceConfigNode);
        setOptions(spaceConfigPath.append(new Path("/Product")), LatticeProduct.getNames());
    }

    public List<String> getOpitions(Path zkPath) {
        ReadWriteLock lock = locks.get(zkPath);
        lock.readLock().lock();
        try {
            return optionMap.get(zkPath);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setOptions(Path zkPath, List<String> options) {
        addLock(zkPath);
        ReadWriteLock lock = locks.get(zkPath);
        lock.writeLock().lock();
        try {
            optionMap.put(zkPath, options);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private synchronized void addLock(Path zkPath) {
        if (!locks.containsKey(zkPath)) {
            locks.put(zkPath, new ReentrantReadWriteLock());
        }
    }

    public SerializableDocumentDirectory applyDynamicBinding(SerializableDocumentDirectory sDir) {
        Iterator<SerializableDocumentDirectory.Node> iter = sDir.getDepthFirstIterator();
        while(iter.hasNext()) {
            SerializableDocumentDirectory.Node node = iter.next();
            Path fullPath = new Path(sDir.getRootPath()).append(node.path);
            if (this.optionMap.containsKey(fullPath)) {
                bindOptions(node, getOpitions(fullPath));
            }
        }
        return sDir;
    }

    private void bindOptions(SerializableDocumentDirectory.Node node, List<String> options) {
        SerializableDocumentDirectory.Metadata metadata = node.getMetadata();
        if (metadata == null) metadata = new SerializableDocumentDirectory.Metadata();
        if (metadata.getType() == null || !metadata.getType().equals("options")) metadata.setType("options");
        metadata.setOptions(options);
        node.setMetadata(metadata);
    }

    public SelectableConfigurationDocument applyDynamicBinding(SelectableConfigurationDocument doc) {
        String component = doc.getComponent();
        Path rootPath = PathBuilder.buildServiceConfigSchemaPath(podId, component);
        for (SelectableConfigurationField field: doc.getNodes()) {
            Path fullPath = rootPath.append(new Path(field.getNode()));
            if (this.optionMap.containsKey(fullPath)) {
                field.setOptions(getOpitions(fullPath));
            }
        }
        return doc;
    }

}
