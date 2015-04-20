package com.latticeengines.admin.tenant.batonadapter.testcomponent;

import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.admin.configurationschema.ConfigurationSchemaTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.template.TemplateComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public class TestComponentTestNG extends BatonAdapterBaseDeploymentTestNG {
    
    @Test(groups = "functional")
    public void testInstallation() { }

    @Override
    public Class<? extends LatticeComponent> getLatticeComponentClassToTest() {
        return TemplateComponent.class;
    }

    @Override
    public Map<String, String> getOverrideProperties() { return null; }

    @Override
    public void testGetDefaultConfig(SerializableDocumentDirectory sDir) {
        ConfigurationSchemaTestNGBase.assertSerializableDirAndJsonAreEqual(sDir, "testcomponent_expected.json");
    }
}
