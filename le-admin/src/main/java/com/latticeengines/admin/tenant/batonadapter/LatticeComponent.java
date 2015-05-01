package com.latticeengines.admin.tenant.batonadapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceProperties;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public abstract class LatticeComponent implements HasName, GraphNode {
    private static Log log = LogFactory.getLog(LatticeComponent.class);

    protected Collection<? extends LatticeComponent> dependencies = new HashSet<>();

    protected static BatonService batonService = new BatonServiceImpl();

    public abstract boolean doRegistration();

    public abstract CustomerSpaceServiceInstaller getInstaller();

    public abstract CustomerSpaceServiceUpgrader getUpgrader();

    public abstract String getVersionString();

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

    protected boolean uploadDefaultConfigAndSchemaByJson(String defaultJson, String metadataJson) {
        String podId = CamilleEnvironment.getPodId();
        Camille camille = CamilleEnvironment.getCamille();

        Path defaultRootPath = PathBuilder.buildServiceDefaultConfigPath(podId, this.getName());
        // deserialize and upload configuration json
        DocumentDirectory dir = LatticeComponent.constructConfigDirectory(defaultJson, metadataJson);
        try {
            camille.delete(defaultRootPath);
        } catch (Exception e) {
            //ignore
        }
        batonService.loadDirectory(dir, defaultRootPath);

        // deserialize and upload metadata json
        Path metadataRootPath = PathBuilder.buildServiceConfigSchemaPath(podId, this.getName());
        dir = LatticeComponent.constructMetadataDirectory(defaultJson, metadataJson);
        try {
            camille.delete(metadataRootPath);
        } catch (Exception e) {
            //ignore
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
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultJson),
                    "UTF-8"
            );
            String metaStr = null;
            if (metadataJson != null) {
                metaStr = IOUtils.toString(
                        Thread.currentThread().getContextClassLoader().getResourceAsStream(metadataJson),
                        "UTF-8"
                );
            }
            SerializableDocumentDirectory sDir =  new SerializableDocumentDirectory(configStr, metaStr);
            return SerializableDocumentDirectory.deserialize(sDir);
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }

    public static DocumentDirectory constructMetadataDirectory(String defaultJson, String metadataJson) {
        try {
            String configStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultJson),
                    "UTF-8"
            );
            String metaStr = null;
            if (metadataJson != null) {
                metaStr = IOUtils.toString(
                        Thread.currentThread().getContextClassLoader().getResourceAsStream(metadataJson),
                        "UTF-8"
                );
            }
            SerializableDocumentDirectory sDir =  new SerializableDocumentDirectory(configStr, metaStr);
            return sDir.getMetadataAsDirectory();
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }

    @Override
    public List<? extends LatticeComponent> getChildren(){ return new ArrayList<>(dependencies); }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) { visitor.visit(this, ctx); }

}
