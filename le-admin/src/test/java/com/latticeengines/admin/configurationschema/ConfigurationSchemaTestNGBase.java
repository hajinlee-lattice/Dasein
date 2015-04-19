package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
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

@Test
class ConfigurationSchemaTestNGBase {

    protected BatonService batonService = new BatonServiceImpl();
    private ObjectMapper objectMapper = new ObjectMapper();
    protected Camille camille;
    protected String podId;
    protected String defaultJson, expectedJson; // required
    protected String metadataJson;              // optional
    protected String componentName;
    protected Path defaultRootPath, metadataRootPath;
    protected DocumentDirectory configDir;
    protected SerializableDocumentDirectory serializableConfigDir;

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

    protected void runMainFlow() {
        // download from camille
        DocumentDirectory storedDir = camille.getDirectory(defaultRootPath);
        storedDir.makePathsLocal();
        // serialize downloaded directory
        SerializableDocumentDirectory serializableDir = new SerializableDocumentDirectory(storedDir);
        // download metadata directory
        DocumentDirectory metaDir = camille.getDirectory(metadataRootPath);
        // apply metadata to downloaded config dir
        serializableDir.applyMetadata(metaDir);

        try {
            String jsonStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(this.expectedJson),
                    "UTF-8"
            );
            Assert.assertEquals(objectMapper.valueToTree(serializableDir), objectMapper.readTree(jsonStr));
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }

    protected void setupPaths() {
        if (this.componentName == null) { throw new AssertionError("Must define component name before setting up paths."); }
        this.defaultRootPath = PathBuilder.buildServiceDefaultConfigPath(podId, componentName);
        this.metadataRootPath = PathBuilder.buildServiceConfigSchemaPath(podId, componentName);
    }

    protected void uploadDirectory() {
        // deserialize and upload configuration json
        DocumentDirectory dir = constructConfigDirectory();
        batonService.loadDirectory(dir, defaultRootPath);

        // deserialize and upload metadata json
        dir = constructMetadataDirectory();
        batonService.loadDirectory(dir, metadataRootPath);
    }

    private DocumentDirectory constructConfigDirectory() {
        try {
            String configStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(this.defaultJson),
                    "UTF-8"
            );
            String metaStr = null;
            if (this.metadataJson != null) {
                metaStr = IOUtils.toString(
                        Thread.currentThread().getContextClassLoader().getResourceAsStream(this.metadataJson),
                        "UTF-8"
                );
            }
            SerializableDocumentDirectory sDir =  new SerializableDocumentDirectory(configStr, metaStr);
            return SerializableDocumentDirectory.deserialize(sDir);
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }

    private DocumentDirectory constructMetadataDirectory() {
        try {
            String configStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(this.defaultJson),
                    "UTF-8"
            );
            String metaStr = null;
            if (this.metadataJson != null) {
                metaStr = IOUtils.toString(
                        Thread.currentThread().getContextClassLoader().getResourceAsStream(this.metadataJson),
                        "UTF-8"
                );
            }
            SerializableDocumentDirectory sDir =  new SerializableDocumentDirectory(configStr, metaStr);
            return sDir.getMetadataAsDirectory();
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }

    private static void assertDirectoryJsonEquals(JsonNode actualJson, JsonNode expectedJson) {
        Assert.assertEquals(actualJson.get("Nodes"), expectedJson.get("Nodes"));
    }
}
