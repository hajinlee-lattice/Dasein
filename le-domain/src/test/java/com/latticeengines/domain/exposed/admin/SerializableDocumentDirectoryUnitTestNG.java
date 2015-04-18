package com.latticeengines.domain.exposed.admin;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;

public class SerializableDocumentDirectoryUnitTestNG {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test(groups = "unit")
    public void testSerializeEmptyDirectory() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);
        String expected = "{\"RootPath\":\"/root\"}";
        Assert.assertEquals(objectMapper.writeValueAsString(serializedConfigDir), expected);
    }

    @Test(groups = "unit")
    public void testSerializeDirectory() throws JsonProcessingException {
        DocumentDirectory dir = new DocumentDirectory(new Path("/root"));
        dir.add("/prop1", "value1");
        dir.add("/prop1/prop11", "value11");
        dir.add("/prop1/prop12", "123");
        SerializableDocumentDirectory serializedDir = new SerializableDocumentDirectory(dir);
        String expected = "{\"RootPath\":\"/root\",\"Nodes\":[{\"Node\":\"prop1\",\"Data\":\"value1\",\"Version\":-1,\"Children\":[{\"Node\":\"prop11\",\"Data\":\"value11\",\"Version\":-1},{\"Node\":\"prop12\",\"Data\":\"123\",\"Version\":-1}]}]}";
        Assert.assertEquals(objectMapper.writeValueAsString(serializedDir), expected);
    }

    @Test(groups = "unit")
    public void testApplyMetadata() throws IOException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        configDir.add("/prop", "");
        configDir.add("/prop/prop1", "1.23");
        configDir.add("/prop2", "true");

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/root"));
        metaDir.add("/prop", "");
        metaDir.add("/prop/prop1", "{\"Type\": \"number\"}");
        metaDir.add("/prop2", "");
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);

        JsonNode jNode = serializedConfigDir.applyMetadata(metaDir);
        String expected = "{\"RootPath\":\"/root\",\"Nodes\":[{\"Node\":\"prop\",\"Version\":-1,\"Children\":[{\"Node\":\"prop1\",\"Data\":\"1.23\",\"Version\":-1}]},{\"Node\":\"prop2\",\"Data\":true,\"Version\":-1}]}";
        Assert.assertEquals(jNode.toString(), expected);
    }

    @Test(groups = "unit")
    public void testDeserialize() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
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
    public void testDeserializeFromJson() throws IOException {
        String json = "{\"RootPath\":\"/root\",\"Nodes\":[{\"Node\":\"prop\",\"Children\":[{\"Node\":\"prop1\",\"Data\":\"1.23\",\"Version\":-1}]},{\"Node\":\"prop2\",\"Data\":true,\"Version\":-1}]}";

        DocumentDirectory expected = new DocumentDirectory(new Path("/root"));
        expected.add("/prop", "");
        expected.add("/prop/prop1", "1.23");
        expected.add("/prop2", "true");

        DocumentDirectory deserializedDir = SerializableDocumentDirectory.deserialize(json);
        Assert.assertTrue(deserializedDir.equals(expected));

        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(deserializedDir);
        String expectedStr = "{\"RootPath\":\"/root\",\"Nodes\":[{\"Node\":\"prop\",\"Version\":-1,\"Children\":[{\"Node\":\"prop1\",\"Data\":\"1.23\",\"Version\":-1}]},{\"Node\":\"prop2\",\"Data\":true,\"Version\":-1}]}";
        Assert.assertEquals(serializedConfigDir.toJson().toString(), expectedStr);
    }
}
