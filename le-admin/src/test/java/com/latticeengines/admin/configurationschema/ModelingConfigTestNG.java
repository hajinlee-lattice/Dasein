package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class ModelingConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = {"unit", "functional"})
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "modeling_default.json";
        this.metadataJson = "modeling_metadata.json";
        this.expectedJson = "modeling_expected.json";
        setupPaths();
        uploadDirectory();
    }

    @Test(groups = "unit")
    public void testUnitMainFlow() { runUnitMainFlow(); }

    @Test(groups = "functional")
    public void testDefaultConfigurationFuncational() { runFunctionalMainFlow(); }

    /*
    ================================================================================
        Test how you want to use the configuration
    ================================================================================
    */

    /**
     * this test demonstrate how to get configuration using DocumentDirectory
     */
    @Test(groups = "unit")
    public void testConfig() throws IOException {
        DocumentDirectory dir = batonService.getDefaultConfiguration(this.component.getName());

        String featuresThreshold = dir.get("/FeaturesThreshold").getDocument().getData();

        Assert.assertEquals(featuresThreshold, "-1");

        DocumentDirectory.Node node = dir.get("/FeaturesThreshold");
        node.getDocument().setData(String.valueOf(10));

        featuresThreshold = dir.get("/FeaturesThreshold").getDocument().getData();

        Assert.assertEquals(featuresThreshold, "10");
    }

}