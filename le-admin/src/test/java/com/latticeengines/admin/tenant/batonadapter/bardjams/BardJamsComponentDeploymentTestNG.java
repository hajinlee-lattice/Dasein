package com.latticeengines.admin.tenant.batonadapter.bardjams;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.admin.service.ServiceService;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.service.impl.ComponentOrchestrator;
import com.latticeengines.admin.service.impl.TenantServiceImpl.ProductAndExternalAdminInfo;
import com.latticeengines.admin.tenant.batonadapter.BatonAdapterDeploymentTestNGBase;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponentDeploymentTestNG;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public class BardJamsComponentDeploymentTestNG extends BatonAdapterDeploymentTestNGBase {
    @Autowired
    private ServiceService serviceService;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private BardJamsEntityMgr bardJamsEntityMgr;

    @Autowired
    private ComponentOrchestrator orchestrator;

    @Autowired
    private VisiDBDLComponentDeploymentTestNG visiDBDLComponentDeploymentTestNG;

    @Value("${admin.test.dl.url}")
    private String dlUrl;

    private SpaceConfiguration spaceConfig;
    private DocumentDirectory vdbdlConfig;
    private SerializableDocumentDirectory jamsConfig;

    @BeforeClass(groups = { "functional", "deployment" })
    @Override
    public void setup() throws Exception {
        super.setup();
        spaceConfig = tenantService.getTenant(contractId, tenantId).getSpaceConfig();
        spaceConfig.setDlAddress(dlUrl);
        tenantService.setupSpaceConfiguration(contractId, tenantId, spaceConfig);
        vdbdlConfig = visiDBDLComponentDeploymentTestNG.getVisiDBDLDocumentDirectory();
        jamsConfig = serviceService.getDefaultServiceConfig(BardJamsComponent.componentName);
        DocumentDirectory metaDir = serviceService.getConfigurationSchema(BardJamsComponent.componentName);
        jamsConfig.applyMetadata(metaDir);
        jamsConfig.setRootPath("/" + BardJamsComponent.componentName);
    }

    @AfterClass(groups = { "functional", "deployment" })
    public void tearDown() throws Exception {
        try {
            deleteTenant(contractId, tenantId);
        } catch (Exception e) {
            // ignore
        }
        deleteBardJamsTenant(tenantId);
    }

    @BeforeMethod(groups = { "functional", "deployment" })
    public void beforeMethod() {
        deleteBardJamsTenant(tenantId);
    }

    @Test(groups = "deployment")
    public void testInstallation() {
        orchestrateVisiDBAndBardJams();

        // wait a while, then test your installation
        BootstrapState state = waitUntilStateIsNotInitial(contractId, tenantId, BardJamsComponent.componentName);
        Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);

        // idempotent test
        Path servicePath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), contractId,
                tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, BardJamsComponent.componentName);
        try {
            CamilleEnvironment.getCamille().delete(servicePath);
        } catch (Exception e) {
            // ignore
        }
        bootstrap(SerializableDocumentDirectory.deserialize(jamsConfig));
        // wait a while, then test your installation
        state = waitUntilStateIsNotInitial(contractId, tenantId, BardJamsComponent.componentName);
        try {
            Assert.assertEquals(state.state, BootstrapState.State.OK, state.errorMessage);
        } catch (AssertionError e) {
            Assert.fail("Idempotent test failed.", e);
        }
    }

    @Test(groups = "functional")
    public void testConvertDocDirToTenant() {
        // send to bootstrapper message queue
        DocumentDirectory confDir = SerializableDocumentDirectory.deserialize(jamsConfig);

        BardJamsTenant tenant = BardJamsComponent.getTenantFromDocDir(confDir, tenantId, spaceConfig, vdbdlConfig);
        verifyTenantCRUD(tenant);

        // construct from overwritten properties
        confDir = SerializableDocumentDirectory.deserialize(jamsConfig);
        tenant = BardJamsComponent.getTenantFromDocDir(confDir, tenantId, spaceConfig, vdbdlConfig);
        verifyTenantCRUD(tenant);
    }

    public void orchestrateVisiDBAndBardJams() {
        Map<String, Map<String, String>> properties = new HashMap<>();
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(vdbdlConfig);
        sDir.setRootPath("/" + VisiDBDLComponent.componentName);
        properties.put(VisiDBDLComponent.componentName, sDir.flatten());

        properties.put(getServiceName(), jamsConfig.flatten());
        ProductAndExternalAdminInfo prodAndExternalAminInfo = super.generateLPAandEmptyExternalAdminInfo();
        orchestrator.orchestrate(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, properties,
                prodAndExternalAminInfo);
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

    private void verifyTenantCRUD(BardJamsTenant tenant) {
        bardJamsEntityMgr.create(tenant);
        verifyBardJamsTenant(tenant.getTenant());
        deleteBardJamsTenant(tenant.getTenant());
    }

    public void verifyBardJamsTenant(String tenant) {
        BardJamsTenant newTenant = bardJamsEntityMgr.findByTenant(tenant);
        Assert.assertNotNull(newTenant);
    }

    public void deleteBardJamsTenant(String tenant) {
        BardJamsTenant existingTenant = bardJamsEntityMgr.findByTenant(tenant);
        if (existingTenant != null) {
            bardJamsEntityMgr.delete(existingTenant);
        }
        existingTenant = bardJamsEntityMgr.findByTenant(tenant);
        Assert.assertNull(existingTenant);
    }

    @Override
    protected String getServiceName() {
        return BardJamsComponent.componentName;
    }
}
