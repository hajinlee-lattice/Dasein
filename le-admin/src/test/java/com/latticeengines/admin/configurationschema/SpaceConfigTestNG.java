package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;

public class SpaceConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = {"unit", "functional"})
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "space_default.json";
        this.metadataJson = "space_metadata.json";  // optional
        this.expectedJson = "space_expected.json";
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
//        DocumentDirectory dir = batonService.getDefaultConfiguration(this.component.getName());
//
//        String adminEmails = dir.get("/AdminEmails").getDocument().getData();
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode aNode = mapper.readTree(adminEmails);
//        if (!aNode.isArray()) {
//            throw new AssertionError("AdminEmails suppose to be a list of strings");
//        }
//        boolean containsRob = false;
//        for (JsonNode node : aNode) {
//            containsRob = containsRob || node.asText().equals("rswarts@lattice-engines.com");
//        }
//        Assert.assertTrue(containsRob);
    }
}
