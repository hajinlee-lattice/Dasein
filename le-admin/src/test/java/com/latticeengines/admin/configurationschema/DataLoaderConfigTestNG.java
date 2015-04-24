package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class DataLoaderConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = {"unit", "functional"})
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "dl_default.json";
        this.metadataJson = "dl_metadata.json";  // optional
        this.expectedJson = "dl_expected.json";
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
    public void testConfig4() throws IOException {
        DocumentDirectory dir = batonService.getDefaultConfiguration(this.component.getName());

        String config4 = dir.get("/Config4").getDocument().getData();
        Assert.assertTrue(Boolean.valueOf(config4));

        String config2 = dir.get("/Config2").getDocument().getData();
        Properties properties = new ObjectMapper().readValue(config2, Properties.class);
        Assert.assertEquals(properties.property1, "value1");
        Assert.assertEquals(properties.property2, "value2");
    }

    private static class Properties {
        @JsonProperty("property1")
        public String property1;

        @JsonProperty("property2")
        public String property2;
    }

}
