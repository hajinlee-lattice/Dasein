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
    
    private static Map<String, LatticeComponent> componentMap = new HashMap<>();
    
    protected static void register(LatticeComponent component) {
        componentMap.put(component.getName(), component);
    }
    
    public static Map<String, LatticeComponent> getRegisteredServices() {
        scanLatticeComponents();
        return componentMap;
    }
    
    private static void scanLatticeComponents() {
        Reflections reflections = new Reflections("com.latticeengines.admin");

        Set<Class<? extends LatticeComponent>> latticeComponentClasses = reflections.getSubTypesOf(LatticeComponent.class);
        
        for (Class<? extends LatticeComponent> c : latticeComponentClasses) {
            try {
                c.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(String.format("Error instantating LatticeComponent instance %s.", c));
            }
        }
    }
    
    protected LatticeComponent() {
        register(this);
    }
    
    public abstract CustomerSpaceServiceInstaller getInstaller();
    
    public abstract CustomerSpaceServiceUpgrader getUpgrader();
}
