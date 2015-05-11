package com.latticeengines.domain.exposed.admin;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class SpaceConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testSpaceConfig() throws IOException {
        SpaceConfiguration config = new SpaceConfiguration();
        config.setTopology(CRMTopology.MARKETO);

        String serialized = JsonUtils.serialize(config);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jNode = objectMapper.readTree(serialized);
        Assert.assertEquals(jNode.get("Topology").asText(), "Marketo");

        SpaceConfiguration newConfig = objectMapper.readValue(serialized, SpaceConfiguration.class);
        Assert.assertEquals(newConfig.getTopology(), CRMTopology.MARKETO);
    }

    @Test(groups = "unit")
    public void testConvertToDocumentDirectory() throws IOException {
        SpaceConfiguration config = new SpaceConfiguration();
        config.setTopology(CRMTopology.MARKETO);

        DocumentDirectory dir = config.toDocumentDirectory();
        Assert.assertNotNull(dir);
        Assert.assertFalse(dir.getChildren().isEmpty());

        DocumentDirectory.Node node = dir.getChild("Topology");
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getDocument().getData(), "Marketo");
    }
}
