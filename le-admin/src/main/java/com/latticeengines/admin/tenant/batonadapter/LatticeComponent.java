package com.latticeengines.admin.tenant.batonadapter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reflections.Reflections;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public abstract class LatticeComponent implements HasName {
    private static Log log = LogFactory.getLog(LatticeComponent.class);

    public abstract boolean doRegistration();

    public static Map<String, LatticeComponent> getRegisteredServices() {
        return scanLatticeComponents();
    }

    private static Map<String, LatticeComponent> scanLatticeComponents() {
        Map<String, LatticeComponent> componentMap = new HashMap<>();
        Reflections reflections = new Reflections("com.latticeengines.admin");
        Set<Class<? extends LatticeComponent>> latticeComponentClasses = reflections
                .getSubTypesOf(LatticeComponent.class);

        for (Class<? extends LatticeComponent> c : latticeComponentClasses) {
            try {
                LatticeComponent instance = c.newInstance();
                componentMap.put(instance.getName(), instance);
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(String.format("Error instantating LatticeComponent instance %s.", c));
            }
        }

        return componentMap;
    }

    public abstract CustomerSpaceServiceInstaller getInstaller();

    public abstract CustomerSpaceServiceUpgrader getUpgrader();

    public abstract String getVersionString();

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
}
