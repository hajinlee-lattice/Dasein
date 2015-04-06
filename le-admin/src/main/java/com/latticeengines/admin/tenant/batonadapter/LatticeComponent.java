package com.latticeengines.admin.tenant.batonadapter;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public abstract class LatticeComponent implements HasName {
    
    private static Map<String, LatticeComponent> componentLookup = new HashMap<>();
    
    protected static void register(LatticeComponent component) {
        componentLookup.put(component.getName(), component);
    }
    
    protected LatticeComponent() {
        register(this);
    }
    
    public abstract CustomerSpaceServiceInstaller getInstaller();
    
    public abstract CustomerSpaceServiceUpgrader getUpgrader();
}
