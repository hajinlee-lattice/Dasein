package com.latticeengines.admin.tenant.batonadapter.vdb;

import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public class VisiDBComponentTestNG extends BatonAdapterBaseDeploymentTestNG {
    
    @Test(groups = "functional")
    public void testInstallation() {
        
    }

    @Override
    public Map<String, String> getOverrideProperties() {
        return null;
    }

    @Override
    public Class<? extends LatticeComponent> getLatticeComponentClassToTest() {
        return VisiDBComponent.class;
    }

    @Override
    public void testGetDefaultConfig(SerializableDocumentDirectory dir) {
        // TODO Auto-generated method stub
        
    }
}
