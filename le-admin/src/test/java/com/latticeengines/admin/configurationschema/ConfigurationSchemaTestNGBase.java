package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

import jdk.nashorn.internal.ir.ObjectNode;

@Test
class ConfigurationSchemaTestNGBase {

    protected BatonService batonService = new BatonServiceImpl();
    protected Camille camille;
    protected String podId;
    protected String defaultJson, metadataJson, expectedJson;
    protected String componentName;
    protected Path defaultRootPath, metadataRootPath;

    @BeforeMethod(groups = "unit")
    protected void setUp() throws Exception {
        CamilleTestEnvironment.start();
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();
    }

    @AfterMethod(groups = "unit")
    protected void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    protected void testUploadMainFlow() {
        if (defaultJson == null) {
            throw new AssertionError("Required json files are not provided.");
        }

        // deserialize and upload configuration json
        DocumentDirectory dir = readJsonAsDirectory(this.defaultJson);
        batonService.loadDirectory(dir, defaultRootPath);

        // deserialize and upload metadata json
        if (this.metadataJson != null) {
            dir = readJsonAsDirectory(this.metadataJson);
            batonService.loadDirectory(dir, metadataRootPath);
        }

        // download from camille
        DocumentDirectory storedDir = camille.getDirectory(defaultRootPath);
        // serialize downloaded directory
        SerializableDocumentDirectory serializableDir = new SerializableDocumentDirectory(storedDir);
        // download metadata directory
        DocumentDirectory metaDir = camille.getDirectory(metadataRootPath);
        try {
            JsonNode jNode = serializableDir.applyMetadata(metaDir);
            JsonNode expectedNode = readJson(this.expectedJson);
            assertDirectoryJsonEquals(jNode, expectedNode);
        } catch (IOException e) {
            throw new AssertionError("Failed to apply metadata", e);
        }

        // read expected json
        DocumentDirectory expectedDir = readJsonAsDirectory(this.expectedJson);
        expectedDir.makePathsLocal();
        storedDir.makePathsLocal();
        Assert.assertTrue(storedDir.equals(expectedDir));
    }

    protected void setupPaths() {
        if (this.componentName == null) { throw new AssertionError("Must define component name before setting up paths."); }
        this.defaultRootPath = PathBuilder.buildServiceDefaultConfigPath(podId, componentName);
        this.metadataRootPath = PathBuilder.buildServiceConfigSchemaPath(podId, componentName);
    }

    protected static DocumentDirectory readJsonAsDirectory(String fileNameInTestResources) {
        JsonNode jsonNode = readJson(fileNameInTestResources);
        try {
            return SerializableDocumentDirectory.deserialize(jsonNode);
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }

    private static JsonNode readJson(String fileNameInTestResources) {
        try {
            String json = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(fileNameInTestResources),
                    "UTF-8"
            );
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(json);
        } catch (IOException|NullPointerException e) {
            throw new AssertionError(
                    String.format("Could not open the specified json file: %s.", fileNameInTestResources), e);
        }
    }

    private static void assertDirectoryJsonEquals(JsonNode actualJson, JsonNode expectedJson) {
        Assert.assertEquals(actualJson.get("Nodes"), expectedJson.get("Nodes"));
    }
}
