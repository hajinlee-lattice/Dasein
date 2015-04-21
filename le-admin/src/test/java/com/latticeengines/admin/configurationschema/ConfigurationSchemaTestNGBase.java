package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
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
public class ConfigurationSchemaTestNGBase {

    protected final BatonService batonService = new BatonServiceImpl();
    protected Camille camille;
    protected String podId;
    protected String defaultJson, expectedJson; // required
    protected String metadataJson;              // optional
    protected Path defaultRootPath, metadataRootPath;
    protected LatticeComponent component;

    @BeforeMethod(groups = {"unit", "functional"})
    protected void setUp() throws Exception {
        CamilleTestEnvironment.start();
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();
    }

    @AfterMethod(groups = {"unit", "functional"})
    protected void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    protected void runUnitMainFlow() {
        // download from camille
        DocumentDirectory storedDir = camille.getDirectory(defaultRootPath);
        storedDir.makePathsLocal();
        // serialize downloaded directory
        SerializableDocumentDirectory serializableDir = new SerializableDocumentDirectory(storedDir);
        // download metadata directory
        DocumentDirectory metaDir = camille.getDirectory(metadataRootPath);
        // apply metadata to downloaded config dir
        serializableDir.applyMetadata(metaDir);

        assertSerializableDirAndJsonAreEqual(serializableDir, this.expectedJson);
    }

    protected void runFunctionalMainFlow() {
        // download from camille
        DocumentDirectory storedDir = this.component.getInstaller().getDefaultConfiguration(this.component.getName());
        storedDir.makePathsLocal();
        // serialize downloaded directory
        SerializableDocumentDirectory serializableDir = new SerializableDocumentDirectory(storedDir);
        // download metadata directory
        DocumentDirectory metaDir = this.component.getInstaller().getConfigurationSchema(this.component.getName());
        // apply metadata to downloaded config dir
        serializableDir.applyMetadata(metaDir);

        assertSerializableDirAndJsonAreEqual(serializableDir, this.expectedJson);

        batonService.discardService(this.component.getName());
    }

    protected void setupPaths() {
        if (this.component == null) { throw new AssertionError("Must define component before setting up paths."); }
        this.defaultRootPath = PathBuilder.buildServiceDefaultConfigPath(podId, this.component.getName());
        this.metadataRootPath = PathBuilder.buildServiceConfigSchemaPath(podId, this.component.getName());
    }

    protected void uploadDirectory() {
        // deserialize and upload configuration json
        DocumentDirectory dir = LatticeComponent.constructConfigDirectory(this.defaultJson, this.metadataJson);
        batonService.loadDirectory(dir, defaultRootPath);

        // deserialize and upload metadata json
        dir = LatticeComponent.constructMetadataDirectory(this.defaultJson, this.metadataJson);
        batonService.loadDirectory(dir, metadataRootPath);
    }

    public static void assertSerializableDirAndJsonAreEqual(SerializableDocumentDirectory sDir, String jsonFile) {
        try {
            String jsonStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(jsonFile),
                    "UTF-8"
            );
            ObjectMapper objectMapper = new ObjectMapper();
            Assert.assertEquals(objectMapper.valueToTree(sDir), objectMapper.readTree(jsonStr));
        } catch (IOException e) {
            throw new AssertionError("Could not deserialize the input json to a directory.", e);
        }
    }
}
