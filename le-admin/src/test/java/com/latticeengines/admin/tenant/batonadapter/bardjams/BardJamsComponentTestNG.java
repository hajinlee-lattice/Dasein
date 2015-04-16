package com.latticeengines.admin.tenant.batonadapter.bardjams;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.bardjams.BardJamsComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public class BardJamsComponentTestNG extends BatonAdapterBaseDeploymentTestNG {

    @Test(groups = "functional")
    public void testInstallation() {

    }

    @Override
    public Map<String, String> getOverrideProperties() {
        Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put("Tenant", "newTenant2");
        overrideProperties.put("DL_URL", "https://dataloader-prod.lattice-engines.com/Dataloader_PLS/");
        overrideProperties.put("DL_User", "admin.dataloader@lattice-engines.com");
        overrideProperties.put("DL_Password", "adm1nDLpr0d");
        overrideProperties.put("ImmediateFolderStruct", "DanteTesting\\Immediate\\");
        overrideProperties.put("ScheduledFolderStruct", "DataLoader\\DL TEST\\Scheduled Jobs");
        overrideProperties.put("Agent_Name", "10.41.1.247");
        overrideProperties.put("TenantType", "P");
        overrideProperties.put("NotificationEmail", "admin@lattice-engines.com");
        return overrideProperties;
    }

    @Override
    public Class<? extends LatticeComponent> getLatticeComponentClassToTest() {
        return BardJamsComponent.class;
    }

    @Override
    public void testGetDefaultConfig(SerializableDocumentDirectory dir) {
        Assert.assertNotNull(dir);

    }
}
