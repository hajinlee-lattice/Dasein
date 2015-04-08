package com.latticeengines.admin.tenant.batonadapter.pls;

import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;

public class PLSComponentTestNG extends BatonAdapterBaseDeploymentTestNG {
    
    @Test(groups = "functional")
    public void testInstallation() {
        
    }

    @Override
    public Map<String, String> getOverrideProperties() {
        return null;
    }

    @Override
    public Class<? extends LatticeComponent> getLatticeComponentClassToTest() {
        return PLSComponent.class;
    }
}
