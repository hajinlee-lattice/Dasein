package com.latticeengines.domain.exposed.admin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

public class SerializableDocumentDirectoryUnitTestNG {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test(groups = "unit")
    public void testConstructByMap() throws IOException {
        CustomerSpaceProperties spaceProperties = new CustomerSpaceProperties(
          "LPA 2.0", "Lead Prioritization", "12345", "56789"
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("/Config1", "value1");
        properties.put("/Config1/Config1.1", "value1.1");
        properties.put("/Config1/Config1.2", "value1.2");
        properties.put("/Config2", "1.23");
        properties.put("/Config3", "true");
        properties.put("CustomerSpaceProperties", JsonUtils.serialize(spaceProperties));
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(properties);
        Map<String, String> otherProperties = sDir.getOtherProperties();
        String json = otherProperties.get("CustomerSpaceProperties");
        Assert.assertEquals(objectMapper.readTree(json), objectMapper.readTree(JsonUtils.serialize(spaceProperties)));

        CustomerSpaceProperties deserializedProp = JsonUtils.deserialize(json, CustomerSpaceProperties.class);
        Assert.assertEquals(spaceProperties.sandboxSfdcOrgId, deserializedProp.sandboxSfdcOrgId);
        Assert.assertEquals(spaceProperties.sfdcOrgId, deserializedProp.sfdcOrgId);
        Assert.assertEquals(spaceProperties.displayName, deserializedProp.displayName);
        Assert.assertEquals(spaceProperties.description, deserializedProp.description);

        String expected = "{\"RootPath\":\"/\",\"Nodes\":[{\"Node\":\"Config1\",\"Data\":\"value1\",\"Version\":-1,\"Children\":[{\"Node\":\"Config1.1\",\"Data\":\"value1.1\",\"Version\":-1},{\"Node\":\"Config1.2\",\"Data\":\"value1.2\",\"Version\":-1}]},{\"Node\":\"Config2\",\"Data\":\"1.23\",\"Version\":-1},{\"Node\":\"Config3\",\"Data\":\"true\",\"Version\":-1}]}";
        Assert.assertEquals(objectMapper.writeValueAsString(sDir), expected);

    }

    @Test(groups = "unit")
    public void testSerializeEmptyDirectory() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);
        String expected = "{\"RootPath\":\"/root\"}";
        Assert.assertEquals(objectMapper.writeValueAsString(serializedConfigDir), expected);
    }

    @Test(groups = "unit")
    public void testSerializeValue() throws JsonProcessingException {
        String expectedJson = "{\"RootPath\":\"/\",\"Nodes\":[{\"Node\":\"property\",\"Data\":\"value\",\"Version\":-1}]}";
        testSingleValueNode("value", null, expectedJson);

        SerializableDocumentDirectory.Metadata metadata = new SerializableDocumentDirectory.Metadata();
        metadata.setType("number");
        expectedJson = "{\"RootPath\":\"/\",\"Nodes\":[{\"Node\":\"property\",\"Data\":\"1.23\",\"Metadata\":{\"Type\":\"number\"},\"Version\":-1}]}";
        testSingleValueNode("1.23", metadata, expectedJson);

        boolean exception = false;
        try {
            testSingleValueNode("fdsrdc", metadata, expectedJson);
        } catch (IllegalArgumentException e) {
            exception = true;
        }
        Assert.assertTrue(exception);

        metadata = new SerializableDocumentDirectory.Metadata();
        metadata.setType("object");
        expectedJson = "{\"RootPath\":\"/\",\"Nodes\":[{\"Node\":\"property\",\"Data\":\"{\\\"Field1\\\":\\\"value1\\\",\\\"Field2\\\":1.23}\",\"Metadata\":{\"Type\":\"object\"},\"Version\":-1}]}";
        ObjectNode oNode = new ObjectMapper().createObjectNode();
        oNode.put("Field1", "value1");
        oNode.put("Field2", 1.23);

        testSingleValueNode(oNode.toString(), metadata, expectedJson);

        exception = false;
        try {
            testSingleValueNode("string", metadata, expectedJson);
        } catch (IllegalArgumentException e) {
            exception = true;
        }
        Assert.assertTrue(exception);
    }

    private void testSingleValueNode(String value, SerializableDocumentDirectory.Metadata metadata, String expectedJson) throws JsonProcessingException {
        DocumentDirectory dir = new DocumentDirectory(new Path("/"));
        dir.add("/property", value);
        SerializableDocumentDirectory serializedDir = new SerializableDocumentDirectory(dir);

        DocumentDirectory metaDir = null;
        if (metadata != null) {
            metaDir = new DocumentDirectory(new Path("/"));
            metaDir.add("/property", metadata.toString());
        }
        serializedDir.applyMetadata(metaDir);

        Assert.assertEquals(objectMapper.writeValueAsString(serializedDir), expectedJson);
    }

    @Test(groups = "unit")
    public void testDeserialize() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/"));
        configDir.add("/prop", "");
        configDir.add("/prop/prop1", "1.23");
        configDir.add("/prop/prop2", "1.23");
        configDir.add("/prop2", "value2");
        configDir.add("/prop2/prop1", "value2");
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);
        DocumentDirectory deserializedDir = SerializableDocumentDirectory.deserialize(serializedConfigDir);
        Assert.assertTrue(deserializedDir.equals(configDir));
    }

    @Test(groups = "unit")
    public void testConstructFromTwoJsons() throws IOException {
        String defaultJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/default.json"),
                "UTF-8"
        );
        String metadataJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/metadata.json"),
                "UTF-8"
        );
        String expectedJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/expected.json"),
                "UTF-8"
        );
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(defaultJson, metadataJson);
        Assert.assertEquals(objectMapper.valueToTree(sDir), objectMapper.readTree(expectedJson));

        // test null metadata directory
        expectedJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/expected_nometa.json"),
                "UTF-8"
        );
        sDir = new SerializableDocumentDirectory(defaultJson);
        Assert.assertEquals(objectMapper.valueToTree(sDir), objectMapper.readTree(expectedJson));
    }

    @Test(groups = "unit")
    public void testStripeOutMetadataDirectory() throws IOException {
        String defaultJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/default.json"),
                "UTF-8"
        );
        String metadataJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/metadata.json"),
                "UTF-8"
        );
        String expectedMetadataJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/expected_metadata.json"),
                "UTF-8"
        );
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(defaultJson, metadataJson);
        DocumentDirectory metaDir = sDir.getMetadataAsDirectory();
        SerializableDocumentDirectory metaSDir = new SerializableDocumentDirectory(metaDir);
        Assert.assertEquals(objectMapper.valueToTree(metaSDir), objectMapper.readTree(expectedMetadataJson));
    }
}
