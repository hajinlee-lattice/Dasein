package com.latticeengines.admin.tenant.batonadapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public abstract class LatticeComponent implements HasName, GraphNode {

    protected Collection<? extends LatticeComponent> dependencies = new HashSet<>();

    protected static BatonService batonService = new BatonServiceImpl();

    @Value("${admin.upload.schema}")
    protected boolean uploadSchema;

    protected Set<LatticeProduct> products;

    public static final String spaceConfigNode = PathConstants.SPACECONFIGURATION_NODE;

    public abstract boolean doRegistration();

    public abstract CustomerSpaceServiceInstaller getInstaller();

    public abstract CustomerSpaceServiceUpgrader getUpgrader();

    public abstract String getVersionString();

    public void setAssociatedProducts(Set<LatticeProduct> products) {
        this.products = products;
    }

    public Set<LatticeProduct> getAssociatedProducts() {
        return this.products;
    }

    public final boolean register() {
        if (doRegistration()) {
            ServiceProperties serviceProps = new ServiceProperties();
            serviceProps.dataVersion = 1;
            serviceProps.versionString = getVersionString();
            ServiceInfo serviceInfo = new ServiceInfo(serviceProps, //
                    getInstaller(), //
                    getUpgrader(), //
                    null);
            ServiceWarden.registerService(getName(), serviceInfo);
            return true;
        }
        return false;
    }

    @PostConstruct
    public void postConstruct() {
        boolean needToRegister = Boolean.valueOf(System.getProperty("com.latticeengines.registerBootstrappers"));
        if (needToRegister && !batonService.getRegisteredServices().contains(getName()))
            register();
    }

    protected boolean uploadDefaultConfigAndSchemaByJson(String defaultJson, String metadataJson) {
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson, this.getName());
    }

    public static boolean uploadDefaultConfigAndSchemaByJson(String defaultJson, String metadataJson, String serviceName) {
        String podId = CamilleEnvironment.getPodId();
        Camille camille = CamilleEnvironment.getCamille();

        Path defaultRootPath = PathBuilder.buildServiceDefaultConfigPath(podId, serviceName);
        // deserialize and upload configuration json
        DocumentDirectory dir = LatticeComponent.constructConfigDirectory(defaultJson, metadataJson);
        try {
            camille.delete(defaultRootPath);
        } catch (Exception e) {
            // ignore
        }
        batonService.loadDirectory(dir, defaultRootPath);

        // deserialize and upload metadata json
        Path metadataRootPath = PathBuilder.buildServiceConfigSchemaPath(podId, serviceName);
        dir = LatticeComponent.constructMetadataDirectory(defaultJson, metadataJson);
        try {
            camille.delete(metadataRootPath);
        } catch (Exception e) {
            // ignore
        }
        batonService.loadDirectory(dir, metadataRootPath);

        try {
            return camille.exists(defaultRootPath) && camille.exists(metadataRootPath);
        } catch (Exception e) {
            return false;
        }
    }

    public static DocumentDirectory constructConfigDirectory(String defaultJson, String metadataJson) {
        try {
            String configStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultJson), "UTF-8");
            String metaStr = null;
            if (metadataJson != null) {
                metaStr = IOUtils.toString(
                        Thread.currentThread().getContextClassLoader().getResourceAsStream(metadataJson), "UTF-8");
            }
            SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configStr, metaStr);
            return SerializableDocumentDirectory.deserialize(sDir);
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }

    public static DocumentDirectory constructMetadataDirectory(String defaultJson, String metadataJson) {
        try {
            String configStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultJson), "UTF-8");
            String metaStr = null;
            if (metadataJson != null) {
                metaStr = IOUtils.toString(
                        Thread.currentThread().getContextClassLoader().getResourceAsStream(metadataJson), "UTF-8");
            }
            SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configStr, metaStr);
            return sDir.getMetadataAsDirectory();
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }

    public SerializableDocumentDirectory getSerializableDefaultConfiguration() {
        DocumentDirectory confDir = batonService.getDefaultConfiguration(getName());
        DocumentDirectory metaDir = batonService.getConfigurationSchema(getName());
        confDir.makePathsLocal();
        metaDir.makePathsLocal();
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(confDir);
        sDir.applyMetadata(metaDir);
        return sDir;
    }

    protected static String getDataWithFailover(String data, String failover) {
        if (data == null || StringUtils.isEmpty(data)) {
            return failover;
        } else {
            return data;
        }
    }

    @Override
    public List<? extends LatticeComponent> getChildren() {
        return new ArrayList<>(dependencies);
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("dependencies", dependencies);
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

}
