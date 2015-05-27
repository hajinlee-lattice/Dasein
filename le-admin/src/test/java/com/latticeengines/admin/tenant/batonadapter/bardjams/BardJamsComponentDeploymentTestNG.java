package com.latticeengines.admin.tenant.batonadapter.bardjams;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;

public class BardJamsComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {

    private final String tenantName = "BardJams Test Tenant";

    @Autowired
    private ServiceService serviceService;

    @Autowired
    private TenantService tenantService;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        // TenantInfo
        TenantProperties tenantProperties = new TenantProperties();
        tenantProperties.description = "Test tenant";
        tenantProperties.displayName = getServiceName() + " Test Tenant";
        TenantInfo tenantInfo = new TenantInfo(tenantProperties);

        // SpaceInfo
        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties();
        spaceProperties.description = tenantProperties.description;
        spaceProperties.displayName = tenantProperties.displayName;
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(spaceProperties, "{\"Dante\":true}");

        // SpaceConfiguration
        SpaceConfiguration spaceConfiguration = tenantService.getDefaultSpaceConfig();
        spaceConfiguration.setDlAddress(dlUrl);
        spaceConfiguration.setTopology(CRMTopology.ELOQUA);

        // Orchestrate tenant
        TenantRegistration reg =  new TenantRegistration();
        reg.setContractInfo(new ContractInfo(new ContractProperties()));
        reg.setTenantInfo(tenantInfo);
        reg.setSpaceInfo(spaceInfo);
        reg.setSpaceConfig(spaceConfiguration);

        loginAD();
        String url = String.format("%s/admin/tenants/%s?contractId=%s", getRestHostPort(), tenantId, contractId);
        boolean created = restTemplate.postForObject(url, reg, Boolean.class);
        Assert.assertTrue(created);
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            //ignore
        }
    }

    @Test(groups = "deployment", enabled = false)
    public void testInstallation() {
        SerializableDocumentDirectory jamsConfig = new SerializableDocumentDirectory(getOverrideProperties());
        DocumentDirectory metaDir = serviceService.getConfigurationSchema(BardJamsComponent.componentName);
        jamsConfig.applyMetadata(metaDir);
        jamsConfig.setRootPath("/" + BardJamsComponent.componentName);

        // send to bootstrapper message queue
        DocumentDirectory confDir = SerializableDocumentDirectory.deserialize(jamsConfig);
        bootstrap(confDir);

        // wait a while, then test your installation
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, BardJamsComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        // idempotent test
        Path servicePath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, BardJamsComponent.componentName);
        try {
            CamilleEnvironment.getCamille().delete(servicePath);
        } catch (Exception e) {
            // ignore
        }
        bootstrap(confDir);
        // wait a while, then test your installation
        state = waitUntilStateIsNotInitial(contractId, tenantId, BardJamsComponent.componentName);
        try {
            Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        } catch (AssertionError e) {
            Assert.fail("Idempotent test failed.", e);
        }
    }

    public static Map<String, String> getOverrideProperties() {
        Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put("/DL_URL", "https://dataloader-prod.lattice-engines.com/Dataloader_PLS/");
        overrideProperties.put("/DL_User", "admin.dataloader@lattice-engines.com");
        overrideProperties.put("/DL_Password", "adm1nDLpr0d");
        overrideProperties.put("/ImmediateFolderStruct", "DanteTesting\\Immediate\\");
        overrideProperties.put("/ScheduledFolderStruct", "DataLoader\\DL TEST\\Scheduled Jobs");
        overrideProperties.put("/Agent_Name", "10.41.1.247");
        overrideProperties.put("/JAMSUser", "LATTICE\\bviets");
        overrideProperties.put("/TenantType", "P");
        overrideProperties.put("/NotificationEmail", "admin@lattice-engines.com");
        overrideProperties.put("/NotifyEmailJob", "DataLoader");
        overrideProperties.put("/DanteManifestPath", "C:\\dante");
        overrideProperties.put("/Queue_Name", "BODCDEPVJOB999");
        overrideProperties.put("/Dante_Queue_Name", "BODCDEPVJOB888");
        overrideProperties.put("/WeekdaySchedule_Name", "DEP_Weekday");
        overrideProperties.put("/WeekendSchedule_Name", "All_Weekend");
        overrideProperties.put("/Data_LaunchPath", "C:\\BD2_ADEDTBDd70064747nY26263627n12\\Launch");
        overrideProperties.put("/Data_ArchivePath", "\\\\10.41.1.187\\archive");
        overrideProperties.put("/DataLoaderTools_Path", "C:\\DLTools");
        overrideProperties.put("/DanteTool_Path", "D:\\Dante\\Install\\bin");
        overrideProperties.put("/LoadGroupList", "AllLeadInsights");
        return overrideProperties;
    }
    @Override
    protected String getServiceName() { return BardJamsComponent.componentName; }
}
