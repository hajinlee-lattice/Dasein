package com.latticeengines.domain.exposed.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

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
    public void testProductList() throws IOException {
        List<LatticeProduct> products = Arrays.asList(LatticeProduct.BIS, LatticeProduct.LPA, LatticeProduct.PD);
        SpaceConfiguration configuration = new SpaceConfiguration();
        configuration.setProducts(products);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(configuration);
        SpaceConfiguration deserialized = mapper.readValue(json, SpaceConfiguration.class);
        Assert.assertEquals(deserialized.getProducts().size(), products.size());

        SerializableDocumentDirectory sDir = configuration.toSerializableDocumentDirectory();
        SpaceConfiguration constructed = new SpaceConfiguration(sDir.getDocumentDirectory());
        Assert.assertEquals(constructed.getProducts().size(), products.size());
        for (LatticeProduct product: products) {
            Assert.assertTrue(constructed.getProducts().contains(product));
        }

        DocumentDirectory dir = sDir.getDocumentDirectory();
        dir.delete(new Path("/Products"));
        constructed = new SpaceConfiguration(sDir.getDocumentDirectory());
        Assert.assertTrue(constructed.getProducts().isEmpty());

        dir.add(new Path("/Products"), new Document("[\"" + LatticeProduct.LPA.getName() + "\",\""
                + LatticeProduct.BIS.getName() + "\"]"));
        constructed = new SpaceConfiguration(sDir.getDocumentDirectory());
        Assert.assertEquals(constructed.getProducts().size(), 2);
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
