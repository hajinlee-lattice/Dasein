package com.latticeengines.admin.tenant.batonadapter.bardjams;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.BatonAdapterBaseDeploymentTestNG;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public class BardJamsComponentDeploymentTestNG extends BatonAdapterBaseDeploymentTestNG {

    @Test(groups = "functional")
    public void testInstallation() {

    }

    @Override
    public Map<String, String> getOverrideProperties() {
        Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put("Tenant", "PLS_tenant");
        overrideProperties.put("DL_TenantName", "PLS_tenant");
        overrideProperties.put("DL_URL", "https://dataloader-prod.lattice-engines.com/Dataloader_PLS/");
        overrideProperties.put("DL_User", "admin.dataloader@lattice-engines.com");
        overrideProperties.put("DL_Password", "adm1nDLpr0d");
        overrideProperties.put("ImmediateFolderStruct", "DanteTesting\\Immediate\\");
        overrideProperties.put("ScheduledFolderStruct", "DataLoader\\DL TEST\\Scheduled Jobs");
        overrideProperties.put("Agent_Name", "10.41.1.247");
        overrideProperties.put("JAMSUser", "LATTICE\\bviets");
        overrideProperties.put("TenantType", "P");
        overrideProperties.put("NotificationEmail", "admin@lattice-engines.com");
        overrideProperties.put("NotifyEmailJob", "DataLoader");
        overrideProperties.put("DanteManifestPath", "c:\\dante");
        overrideProperties.put("Queue_Name", "BODCDEPVJOB999");
        overrideProperties.put("Dante_Queue_Name", "BODCDEPVJOB888");
        overrideProperties.put("WeekdaySchedule_Name", "DEP_Weekday");
        overrideProperties.put("WeekendSchedule_Name", "All_Weekend");
        overrideProperties.put("Data_LaunchPath", "c:\\BD2_ADEDTBDd70064747nY26263627n12\\Launch");
        overrideProperties.put("Data_ArchivePath", "\\\\10.41.1.187\\archive");
        overrideProperties.put("DataLoaderTools_Path", "C:\\DLTools");
        overrideProperties.put("DanteTool_Path", "D:\\Dante\\Install\\bin");
        overrideProperties.put("LoadGroupList", "AllLeadInsights");
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
