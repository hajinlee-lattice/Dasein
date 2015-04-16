package com.latticeengines.admin.tenant.batonadapter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reflections.Reflections;

import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public abstract class LatticeComponent implements HasName {
    private static Log log = LogFactory.getLog(LatticeComponent.class);

    public boolean doRegistration() {
        return true;
    }

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
}
