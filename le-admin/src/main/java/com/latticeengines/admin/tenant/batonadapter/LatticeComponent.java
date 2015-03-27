package com.latticeengines.admin.tenant.batonadapter;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.camille.exposed.config.bootstrap.Installer;
import com.latticeengines.camille.exposed.config.bootstrap.Upgrader;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public abstract class LatticeComponent implements HasName {
    
    private static Map<String, LatticeComponent> componentLookup = new HashMap<>();
    
    protected static void register(LatticeComponent component) {
        componentLookup.put(component.getName(), component);
    }
    
    protected LatticeComponent() {
        register(this);
    }
    
    public abstract Installer getInstaller();
    
    public abstract Upgrader getUpgrader();
}
