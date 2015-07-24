package com.latticeengines.domain.exposed.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        properties.put("/Config4/Config4.1", "value4.1");
        properties.put("/Config4/Config4.2", "value4.2");
        properties.put("CustomerSpaceProperties", JsonUtils.serialize(spaceProperties));
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(properties);
        DocumentDirectory dir = sDir.getDocumentDirectory();
        dir.makePathsLocal();
        sDir = new SerializableDocumentDirectory(dir);
        Assert.assertEquals(sDir.getNodes().size(), 4);
        Assert.assertEquals(sDir.getDocumentDirectory().getChild("Config4").getChildren().size(), 2);

        Map<String, String> flattendSDir = sDir.flatten();
        Assert.assertEquals(flattendSDir.get("/Config1"), "value1");
    }

    @Test(groups = "unit")
    public void testSerializeEmptyDirectory() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);
        String expected = "{\"RootPath\":\"/root\"}";
        Assert.assertEquals(objectMapper.writeValueAsString(serializedConfigDir), expected);
    }

    @Test(groups = "unit")
    public void testEscapeJson() throws IOException {
        String defaultJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/toescape.json"),
                "UTF-8"
        );
        String metaJson = IOUtils.toString(
                ClassLoader.getSystemResourceAsStream("com/latticeengines/domain/exposed/admin/toescape_meta.json"),
                "UTF-8"
        );

        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(defaultJson, metaJson);
        Assert.assertNotNull(sDir);
        Assert.assertEquals(sDir.getNodeAtPath("/Config1").getData(), "my\\path");
        Assert.assertEquals(sDir.getNodeAtPath("/Config2").getData(), "{\"property1\":\"value1\",\"property2\":\"value2\"}");
        Assert.assertEquals(sDir.getNodeAtPath("/Config3").getData(), "[\"item1\",\"item2\"]");

        DocumentDirectory dir = SerializableDocumentDirectory.deserialize(sDir);
        Assert.assertEquals(dir.get("/Config1").getDocument().getData(), "my\\path");
        Assert.assertEquals(dir.get("/Config2").getDocument().getData(), "{\"property1\":\"value1\",\"property2\":\"value2\"}");
        Assert.assertEquals(dir.get("/Config3").getDocument().getData(), "[\"item1\",\"item2\"]");
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
    public void testSerializePath() throws JsonProcessingException {
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
    public void testEnforcedString() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/"));
        configDir.add("/prop", "true");
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/"));
        metaDir.add("/prop", "{\"Type\":\"string\"}");

        serializedConfigDir.applyMetadata(metaDir);

        DocumentDirectory deserializedDir = SerializableDocumentDirectory.deserialize(serializedConfigDir);
        Assert.assertTrue(deserializedDir.equals(configDir));
    }

    @Test(groups = "unit")
    public void testValueZeroNumber() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/"));
        configDir.add("/prop", "0");
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/"));
        metaDir.add("/prop", "{\"Type\":\"number\"}");

        serializedConfigDir.applyMetadata(metaDir);

        DocumentDirectory deserializedDir = SerializableDocumentDirectory.deserialize(serializedConfigDir);
        Assert.assertTrue(deserializedDir.equals(configDir));

        Assert.assertEquals(deserializedDir.getChild("prop").getDocument().getData(), "0");
    }

    @Test(groups = "unit")
    public void testPasswordType() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/"));
        configDir.add("/prop", "password");
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/"));
        metaDir.add("/prop", "{\"Type\":\"password\"}");

        serializedConfigDir.applyMetadata(metaDir);
        boolean seenTheNode = false;
        for (SerializableDocumentDirectory.Node node: serializedConfigDir.getNodes()) {
            if (node.getNode().equals("prop")) {
                seenTheNode = true;
                Assert.assertEquals(node.getData(), "password");
                SerializableDocumentDirectory.Metadata metadata = node.getMetadata();
                Assert.assertNotNull(metadata);
                Assert.assertEquals(metadata.getType(), "password");
            }
        }
        Assert.assertTrue(seenTheNode);
    }

    @Test(groups = "unit")
    public void testDeserialize() throws JsonProcessingException {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/"));
        configDir.add("/prop", "/var/logs");
        SerializableDocumentDirectory serializedConfigDir = new SerializableDocumentDirectory(configDir);
        DocumentDirectory deserializedDir = SerializableDocumentDirectory.deserialize(serializedConfigDir);
        Assert.assertTrue(deserializedDir.equals(configDir));

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/"));
        metaDir.add("/prop", "{\"Type\":\"path\"}");

        serializedConfigDir.applyMetadata(metaDir);

        for (SerializableDocumentDirectory.Node node : serializedConfigDir.getNodes()) {
            if (node.getNode().equals("prop")) {
                SerializableDocumentDirectory.Metadata metadata = node.getMetadata();
                Assert.assertNotNull(metadata);
                Assert.assertEquals(metadata.getType(), "path");
            }
        }

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

    @Test(groups = "unit")
    public void testGetOptionalFields() throws JsonProcessingException {
        //==================================================
        // main usage
        //==================================================
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        configDir.add("/Config1", "option1");
        configDir.add("/Config2", "otherOption1");

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/root"));

        SerializableDocumentDirectory.Metadata metadata = new SerializableDocumentDirectory.Metadata();
        metadata.setType("options");
        metadata.setOptions(Arrays.asList("option1", "option2", "option3"));
        metaDir.add("/Config1", metadata.toString());

        metadata = new SerializableDocumentDirectory.Metadata();
        metadata.setType("options");
        metadata.setOptions(Arrays.asList("otherOption1", "otherOption2"));
        metaDir.add("/Config2", metadata.toString());


        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadata(metaDir);

        Assert.assertEquals(sDir.getNodes().size(), 2);

        for (SerializableDocumentDirectory.Node node : sDir.getNodes()) {
            if (node.getNode().equals("Config1")) {
                metadata = node.getMetadata();
                Assert.assertNotNull(metadata);
                Assert.assertEquals(metadata.getType(), "options");
                Assert.assertEquals(metadata.getOptions().size(), 3);
            } else if (node.getNode().equals("Config2")) {
                metadata = node.getMetadata();
                Assert.assertNotNull(metadata);
                Assert.assertEquals(metadata.getType(), "options");
                Assert.assertEquals(metadata.getOptions().size(), 2);
            }
        }

        List<SelectableConfigurationField> optionalFields = sDir.findSelectableFields();

        Assert.assertEquals(optionalFields.size(), 2);
        for (SelectableConfigurationField field : optionalFields) {
            if (field.getNode().equals("/Config1")) {
                Assert.assertEquals(field.getOptions().size(), 3);
            } else if (field.getNode().equals("/Config2")) {
                Assert.assertEquals(field.getOptions().size(), 2);
            }
        }

        //==================================================
        // invalid option
        //==================================================
        configDir = new DocumentDirectory(new Path("/root"));
        configDir.add("/Config", "option1");

        metaDir = new DocumentDirectory(new Path("/root"));
        metadata = new SerializableDocumentDirectory.Metadata();
        metadata.setType("options");
        metadata.setOptions(Arrays.asList("option1", "option2", "option3"));
        metaDir.add("/Config", metadata.toString());

        sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadata(metaDir);

        Collection<SerializableDocumentDirectory.Node> nodes = sDir.getNodes();
        for (SerializableDocumentDirectory.Node node: nodes) {
            node.setData("option4");
        }

        optionalFields = sDir.findSelectableFields();
        Assert.assertEquals(optionalFields.size(), 0);

        //==================================================
        // child node
        //==================================================
        configDir = new DocumentDirectory(new Path("/root"));
        configDir.add("/Parent", "");
        configDir.add("/Parent/Child1", "string");
        configDir.add("/Parent/Child2", "option1");

        metaDir = new DocumentDirectory(new Path("/root"));
        metadata = new SerializableDocumentDirectory.Metadata();
        metadata.setType("options");
        metadata.setOptions(Arrays.asList("option1", "option2", "option3"));
        metaDir.add("/Parent", "");
        metaDir.add("/Parent/Child2", metadata.toString());

        sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadata(metaDir);

        optionalFields = sDir.findSelectableFields();
        Assert.assertEquals(optionalFields.size(), 1);
        for (SelectableConfigurationField field : optionalFields) {
            if (field.getNode().equals("/Parent/Child2")) {
                Assert.assertEquals(field.getOptions().size(), 3);
            }
        }
    }


    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class )
    public void testValidateOptions() {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        configDir.add("/Config", "option1");

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/root"));
        SerializableDocumentDirectory.Metadata metadata = new SerializableDocumentDirectory.Metadata();
        metadata.setType("options");
        metadata.setOptions(Arrays.asList("option1", "option2", "option3"));
        metaDir.add("/Config", metadata.toString());

        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadata(metaDir);

        metaDir = new DocumentDirectory(new Path("/root"));
        metadata = new SerializableDocumentDirectory.Metadata();
        metadata.setType("options");
        metadata.setOptions(Arrays.asList("option2", "option3"));
        metaDir.add("/Config", metadata.toString());

        sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadataTemplate(metaDir, true);

        sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadata(metaDir);
    }


    @Test(groups = "unit")
    public void testIteratorOfEmptyDirectory() {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        Iterator<SerializableDocumentDirectory.Node> iter = sDir.getBreathFirstIterator();
        Assert.assertFalse(iter.hasNext());
        iter = sDir.getDepthFirstIterator();
        Assert.assertFalse(iter.hasNext());
    }

    @Test(groups = "unit")
    public void testBreathFirstIterator() {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        configDir.add("/Config1", "value1");
        configDir.add("/Config2", "");
        configDir.add("/Config2/Config2.1", "value2.1");
        configDir.add("/Config2/Config2.2", "value2.2");
        configDir.add("/Config3", "value3");

        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        Iterator<SerializableDocumentDirectory.Node> iter = sDir.getBreathFirstIterator();

        Assert.assertTrue(iter.hasNext());
        Set<Path> seenPathes = new HashSet<>();
        while(iter.hasNext()) {
            SerializableDocumentDirectory.Node node = iter.next();
            if (node.path.toString().contains("Config2.")) {
                Assert.assertTrue(seenPathes.contains(new Path("/Config1")));
                Assert.assertTrue(seenPathes.contains(new Path("/Config2")));
                Assert.assertTrue(seenPathes.contains(new Path("/Config3")));
            }
            seenPathes.add(node.path);
        }

        Assert.assertEquals(seenPathes.size(), 5);
    }

    @Test(groups = "unit")
    public void testDepthFirstIterator() {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        configDir.add("/Config1", "value1");
        configDir.add("/Config2", "");
        configDir.add("/Config2/Config2.1", "value2.1");
        configDir.add("/Config2/Config2.2", "value2.2");
        configDir.add("/Config3", "value3");

        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        Iterator<SerializableDocumentDirectory.Node> iter = sDir.getDepthFirstIterator();

        Assert.assertTrue(iter.hasNext());
        Set<Path> seenPathes = new HashSet<>();
        while(iter.hasNext()) {
            SerializableDocumentDirectory.Node node = iter.next();
            if (node.path.equals(new Path("/Config1")) || node.path.equals(new Path("/Config3"))) {
                if (seenPathes.contains(new Path("/Config2"))) {
                    Assert.assertTrue(seenPathes.contains(new Path("/Config2/Config2.1")));
                    Assert.assertTrue(seenPathes.contains(new Path("/Config2/Config2.2")));
                }
            } else if (node.path.toString().contains("Config2.")) {
                Assert.assertTrue(seenPathes.contains(new Path("/Config2")));
            }
            seenPathes.add(node.path);
        }

        Assert.assertEquals(seenPathes.size(), 5);
    }

    @Test(groups = "unit")
    public void testApplyMetadata() {
        DocumentDirectory configDir = new DocumentDirectory(new Path("/root"));
        configDir.add("/Config1", "option1");
        configDir.add("/Config2", "");
        configDir.add("/Config2/Config2.1", "option21");
        configDir.add("/Config2/Config2.2", "option22");

        DocumentDirectory metaDir = new DocumentDirectory(new Path("/root"));
        metaDir.add("/Config1", "{\"Type\":\"options\",\"Options\":[\"option1\",\"option2\",\"option3\"]}");
        metaDir.add("/Config2", "");
        metaDir.add("/Config2/Config2.1", "{\"Type\":\"options\",\"Options\":[\"option21\",\"option22\",\"option23\"]}");
        metaDir.add("/Config2/Config2.2", "{\"Type\":\"options\",\"Options\":[],\"DynamicOptions\":true}");

        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        sDir.applyMetadata(metaDir);

        SerializableDocumentDirectory.Node node = sDir.getNodeAtPath("/Config2/Config2.1");
        Assert.assertNotNull(node.getMetadata());
        Assert.assertEquals(node.getMetadata().getOptions().size(), 3);
    }
}
