package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.tenant.batonadapter.bardjams.BardJamsComponent;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class BardJamsConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = { "unit", "functional" })
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new BardJamsComponent();
        this.defaultJson = "bardjams_default.json";
        this.metadataJson = "bardjams_metadata.json"; // optional
        this.expectedJson = "bardjams_expected.json";
        setupPaths();
        uploadDirectory();
    }

    @Test(groups = "unit")
    public void testUnitMainFlow() {
        runUnitMainFlow();
    }

    @Test(groups = "functional")
    public void testDefaultConfigurationFuncational() {
        runFunctionalMainFlow();
    }

    /*
     * ==========================================================================
     * ====== Test how you want to use the configuration
     * ========================
     * ========================================================
     */

    /**
     * this test demonstrate how to get configuration using DocumentDirectory
     */
    @Test(groups = "unit")
    public void getDefaultConfiguration() throws IOException {
        DocumentDirectory dir = batonService.getDefaultConfiguration(this.component.getName());

        String tenantType = dir.get("/TenantType").getDocument().getData();
        Assert.assertEquals(tenantType, "P");
        String dl_Url = dir.get("/DL_URL").getDocument().getData();
        Assert.assertEquals(dl_Url, "https://dataloader-prod.lattice-engines.com/Dataloader_PLS");
        String immediateFolderStruct = dir.get("/ImmediateFolderStruct").getDocument().getData();
        Assert.assertEquals(immediateFolderStruct, "DanteTesting\\Immediate");
    }

}
