package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.latticeengines.common.exposed.transformer.RecommendationAvroToCsvTransformer;

public class AvroUtilsUnitTestNG {

    private static Logger log = LoggerFactory.getLogger(AvroUtilsUnitTestNG.class);
    
    @SuppressWarnings("deprecation")
    @Test(groups = "unit")
    public void combineSchemas() throws Exception {
        URL url1 = ClassLoader.getSystemResource("com/latticeengines/common/exposed/util/avroUtilsData/schema1.avsc");
        File avroFile1 = new File(url1.getFile());
        URL url2 = ClassLoader.getSystemResource("com/latticeengines/common/exposed/util/avroUtilsData/schema2.avsc");
        File avroFile2 = new File(url2.getFile());
        Schema s1 = Schema.parse(avroFile1);
        Schema s2 = Schema.parse(avroFile2);

        Object[] combinedSchema = AvroUtils.combineSchemas(s1, s2);

        assertEquals(((Schema) combinedSchema[0]).getFields().size(), s1.getFields().size() + s2.getFields().size());

        String uuid = ((Schema) combinedSchema[0]).getProp("uuid");
        assertNotEquals("abc", uuid);
        String uuids = ((Schema) combinedSchema[0]).getProp("uuids");
        assertNotNull(uuids);
        assertEquals("abc,xyz", uuids);

        combinedSchema = AvroUtils.combineSchemas(s1, ((Schema) combinedSchema[0]));
        uuids = ((Schema) combinedSchema[0]).getProp("uuids");
        assertNotNull(uuids);
        assertEquals("abc,abc,xyz", uuids);
    }

    @Test(groups = "unit", dataProvider = "avscFileProvider", enabled = true)
    public void generateHiveCreateTableStatement(String avscFileName) throws Exception {
        URL url = ClassLoader.getSystemResource(
                String.format("com/latticeengines/common/exposed/util/avroUtilsData/%s", avscFileName));
        File avscFile = new File(url.getFile());
        String hiveTableDDL = AvroUtils.generateHiveCreateTableStatement("ABC", "/tmp/Stoplist", avscFile.getPath());
        System.out.println(hiveTableDDL);
    }

    @Test(groups = "unit")
    public void testAlignFields() throws Exception {
        Schema schema1 = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"Shuffled\",\"doc\":\"Testing data\"," + "\"fields\":[" //
                        + "{\"name\":\"Field1\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Field2\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Field3\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Field4\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Field5\",\"type\":[\"int\",\"null\"]}]}");
        Schema schema = AvroUtils.removeFields(schema1, new String[]{ "Field1", "Field2", "Field3" });
        List<Schema.Field> fieldList = schema.getFields();
        Assert.assertEquals(fieldList.size(), 2);
        Assert.assertEquals(fieldList.get(0).name(), "Field4");
        Assert.assertEquals(fieldList.get(1).name(), "Field5");
    }

    @Test(groups = "unit")
    public void testRemoveFields() throws Exception {
        Schema schema1 = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"Shuffled\",\"doc\":\"Testing data\"," + "\"fields\":[" //
                        + "{\"name\":\"Field2\",\"type\":\"int\"}," //
                        + "{\"name\":\"Field1\",\"type\":[\"int\",\"null\"]}]}");
        Schema schema2 = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"Ordered\",\"doc\":\"Testing data\"," + "\"fields\":[" //
                        + "{\"name\":\"Field1\",\"type\":\"int\"}," //
                        + "{\"name\":\"Field2\",\"type\":[\"int\",\"null\"]}]}");
        Schema schema = AvroUtils.alignFields(schema1, schema2);
        List<Schema.Field> fieldList = schema.getFields();
        Assert.assertEquals(schema.getName(), "Shuffled");
        Assert.assertEquals(fieldList.get(0).name(), "Field1");
        Assert.assertEquals(fieldList.get(1).name(), "Field2");
    }

    @Test(groups = "unit")
    public void testGetType() throws Exception {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," + "\"fields\":["
                + "{\"name\":\"Field1\",\"type\":\"int\"}," + "{\"name\":\"Field2\",\"type\":[\"int\",\"null\"]}]}");
        for (Schema.Field field : schema.getFields()) {
            Assert.assertEquals(AvroUtils.getType(field), Schema.Type.INT);
        }

    }

    @Test(groups = "unit")
    public void testConvertSqlServerTypeToAvro() throws IllegalArgumentException, IllegalAccessException {
        Assert.assertEquals(AvroUtils.convertSqlTypeToAvro("NVARCHAR(255)"), Type.STRING);
        Assert.assertEquals(AvroUtils.convertSqlTypeToAvro("date"), Type.LONG);
        Assert.assertEquals(AvroUtils.convertSqlTypeToAvro("BINARY"), Type.BYTES);
        Assert.assertEquals(AvroUtils.convertSqlTypeToAvro("INT"), Type.INT);
        Assert.assertEquals(AvroUtils.convertSqlTypeToAvro("LONG"), Type.LONG);
    }

    @Test(groups = "unit")
    public void isAvroFriendlyFieldName() throws Exception {
        Assert.assertTrue(AvroUtils.isAvroFriendlyFieldName("abc"));
        Assert.assertTrue(AvroUtils.isAvroFriendlyFieldName("_abc"));
        Assert.assertFalse(AvroUtils.isAvroFriendlyFieldName("1abc"));
        Assert.assertFalse(AvroUtils.isAvroFriendlyFieldName("+abc"));
        Assert.assertFalse(AvroUtils.isAvroFriendlyFieldName("/abc"));
        Assert.assertFalse(AvroUtils.isAvroFriendlyFieldName("-abc"));
        Assert.assertFalse(AvroUtils.isAvroFriendlyFieldName(""));
        Assert.assertFalse(AvroUtils.isAvroFriendlyFieldName(null));
    }

    @DataProvider(name = "avscFileProvider")
    public Object[][] getAvscFile() {
        return new Object[][] { { "aps.avsc" }, //
                { "leaccount.avsc" }, //
                { "leaccountextensions.avsc" } };
    }

    @Test(groups = "unit")
    public void testReadSchema() throws Exception {
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/common/exposed/util/avroUtilsData/compressed.avro");
        Schema schema = AvroUtils.readSchemaFromInputStream(is);
        System.out.println(schema.toString(true));
        Assert.assertNotNull(schema);

        is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/common/exposed/util/avroUtilsData/compressed.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(is);
        records.forEach(record -> {
            System.out.println(record.toString());
        });
    }

    @Test(groups = "unit")
    public void convertRecommendationsAvroToJSON() throws IOException {
        URL avroUrl = ClassLoader.getSystemResource("com/latticeengines/common/exposed/util/avroUtilsData/launch_recommendations.avro");
        File jsonFile = File.createTempFile("RecommendationsTest_", ".json");
        AvroUtils.convertAvroToJSON(avroUrl.getFile(), jsonFile, new Function<GenericRecord, GenericRecord>() {
            @Override
            public GenericRecord apply(GenericRecord rec) {
                Object obj = rec.get("CONTACTS");
                if (obj != null && StringUtils.isNotBlank(obj.toString())) {
                    obj = JsonUtils.deserialize(obj.toString(), new TypeReference<List<Map<String, String>>>(){});
                    rec.put("CONTACTS", obj);
                }
                return rec;
            }
        });

        log.info("Created JON File at: " + jsonFile.getAbsolutePath());
        ObjectMapper om = new ObjectMapper();
        try(FileInputStream fis = new FileInputStream(jsonFile)) {
            JsonNode node = om.readTree(fis);
            Assert.assertEquals(node.getNodeType(), JsonNodeType.ARRAY);
            Assert.assertNotNull(node.get(0));
            JsonNode firstRecommendationObject = node.get(0);
            JsonNode contactList = firstRecommendationObject.get("CONTACTS");
            Assert.assertEquals(contactList.getNodeType(), JsonNodeType.ARRAY);
        }
    }

    @Test(groups = "unit")
    public void convertRecommendationsAvroToCSV() throws IOException {
        URL avroUrl = ClassLoader.getSystemResource("com/latticeengines/common/exposed/util/avroUtilsData/launch_recommendations.avro");
        File csvFile = File.createTempFile("RecommendationsTest_", ".csv");

        AvroUtils.convertAvroToCSV(avroUrl.getFile(), csvFile, new RecommendationAvroToCsvTransformer());

        log.info("Created CSV File at: " + csvFile.getAbsolutePath());
        /*
        ObjectMapper om = new ObjectMapper();
        try(FileInputStream fis = new FileInputStream(jsonFile)) {
            JsonNode node = om.readTree(fis);
            Assert.assertEquals(node.getNodeType(), JsonNodeType.ARRAY);
            Assert.assertNotNull(node.get(0));
            JsonNode firstRecommendationObject = node.get(0);
            JsonNode contactList = firstRecommendationObject.get("CONTACTS");
            Assert.assertEquals(contactList.getNodeType(), JsonNodeType.ARRAY);
        }
        */
    }

    @Test(groups = "unit", dataProvider = "columnProvider")
    public void testIsValidColumn(String column, boolean isValid) {
        Assert.assertEquals(AvroUtils.isValidColumn(column), isValid);
    }

    @DataProvider(name = "columnProvider")
    public Object[][] getColumnProvider() {
        return new Object[][] {
                { "", false }, //
                { " ", false }, //
                { "_", false }, //
                { "^?", false }, //

                { "_A", false }, //
                { "_a", false }, //
                { "_9", false }, //

                { "A_", true }, //
                { "a_", true }, //
                { "9_", true }, //

                { "A_A", true }, //
                { "a_a", true }, //
                { "9_9", true }, //

                { "A A", false }, //
                { "a a", false }, //
                { "9 9", false }, //

                { "AA ", false }, //
                { "aa ", false }, //
                { "99 ", false }, //
        };
    }
}
