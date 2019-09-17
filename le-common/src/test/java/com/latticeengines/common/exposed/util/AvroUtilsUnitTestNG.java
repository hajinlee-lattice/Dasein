package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.latticeengines.common.exposed.transformer.RecommendationAvroToCsvTransformer;
import com.latticeengines.common.exposed.transformer.RecommendationAvroToJsonFunction;

import au.com.bytecode.opencsv.CSVReader;

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

    @Test(groups = "unit")
    public void testAlignFields() {
        Schema schema1 = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"Shuffled\",\"doc\":\"Testing data\"," + "\"fields\":[" //
                        + "{\"name\":\"Field1\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Field2\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Field3\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Field4\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Field5\",\"type\":[\"int\",\"null\"]}]}");
        Schema schema = AvroUtils.removeFields(schema1, "Field1", "Field2", "Field3");
        List<Schema.Field> fieldList = schema.getFields();
        Assert.assertEquals(fieldList.size(), 2);
        Assert.assertEquals(fieldList.get(0).name(), "Field4");
        Assert.assertEquals(fieldList.get(1).name(), "Field5");
    }

    @Test(groups = "unit")
    public void testRemoveFields() {
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
    public void testGetType() {
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
    public void isAvroFriendlyFieldName() {
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
        Assert.assertNotNull(is);
        Schema schema = AvroUtils.readSchemaFromInputStream(is);
        System.out.println(schema.toString(true));
        Assert.assertNotNull(schema);

        is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/common/exposed/util/avroUtilsData/compressed.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(is);
        records.forEach(record -> System.out.println(record.toString()));
    }

    @Test(groups = "unit")
    public void tesCount() throws Exception {
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/common/exposed/util/avroUtilsData/compressed.avro");
        Assert.assertNotNull(is);

        String tempDir = "/tmp/AvroUnitTest";
        String avroPath = tempDir + "/compressed.avro";
        FileUtils.deleteQuietly(new File(tempDir));
        FileUtils.copyInputStreamToFile(is, new File(avroPath));

        long count = AvroUtils.count(new Configuration(), avroPath);
        Assert.assertEquals(count, 192);

        FileUtils.deleteQuietly(new File(tempDir));
    }

    @Test(groups = "unit")
    public void convertRecommendationsAvroToJSON() throws IOException {
        URL avroUrl = ClassLoader
                .getSystemResource("com/latticeengines/common/exposed/util/avroUtilsData/launch_recommendations.avro");
        File jsonFile = File.createTempFile("RecommendationsTest_", ".json");
        AvroUtils.convertAvroToJSON(avroUrl.getFile(), jsonFile,
                new RecommendationAvroToJsonFunction(
                        readCsvIntoMap("com/latticeengines/play/launch/account_display_names.csv"),
                        readCsvIntoMap("com/latticeengines/play/launch/contact_display_names.csv")));

        log.info("Created JON File at: " + jsonFile.getAbsolutePath());
        ObjectMapper om = new ObjectMapper();
        try (FileInputStream fis = new FileInputStream(jsonFile)) {
            JsonNode node = om.readTree(fis);
            Assert.assertEquals(node.getNodeType(), JsonNodeType.ARRAY);
            Assert.assertNotNull(node.get(0));
            JsonNode firstRecommendationObject = node.get(0);
            JsonNode contactList = firstRecommendationObject.get("CONTACTS");
            Assert.assertEquals(contactList.getNodeType(), JsonNodeType.ARRAY);
        }
    }

    private Map<String, String> readCsvIntoMap(String filePath) throws IOException {
        Map<String, String> map = new HashMap<>();

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
        String attributeDiplayNames = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
        Scanner scanner = new Scanner(attributeDiplayNames);
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            String[] values = line.split(",");
            map.put(values[0], values[1]);
        }
        return map;
    }

    @Test(groups = "unit")
    public void convertRecommendationsAvroToCSV() throws IOException {
        URL avroUrl = ClassLoader
                .getSystemResource("com/latticeengines/common/exposed/util/avroUtilsData/launch_recommendations.avro");
        File csvFile = File.createTempFile("RecommendationsTest_", ".csv");

        AvroUtils.convertAvroToCSV(avroUrl.getFile(), csvFile,
                new RecommendationAvroToCsvTransformer(
                        readCsvIntoMap("com/latticeengines/play/launch/account_display_names.csv"),
                        readCsvIntoMap("com/latticeengines/play/launch/contact_display_names.csv"), true));

        log.info("Created CSV File at: " + csvFile.getAbsolutePath());
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> csvRows = reader.readAll();
            log.info(String.format("There are %d rows in file %s.", csvRows.size(), csvFile.getName()));
            assertEquals(csvRows.size(), 15);
        }

        AvroUtils.convertAvroToCSV(avroUrl.getFile(), csvFile,
                new RecommendationAvroToCsvTransformer(
                        readCsvIntoMap("com/latticeengines/play/launch/account_display_names.csv"),
                        readCsvIntoMap("com/latticeengines/play/launch/contact_display_names.csv"), false));

        log.info("Created CSV File at: " + csvFile.getAbsolutePath());
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> csvRows = reader.readAll();
            log.info(String.format("There are %d rows in file %s.", csvRows.size(), csvFile.getName()));
            assertEquals(csvRows.size(), 24);
        }
    }

    @Test(groups = "unit", dataProvider = "columnProvider")
    public void testIsValidColumn(String column, boolean isValid) {
        Assert.assertEquals(AvroUtils.isValidColumn(column), isValid);
    }

    @DataProvider(name = "columnProvider")
    public Object[][] getColumnProvider() {
        return new Object[][] { { "", false }, //
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

    @Test(groups = "unit")
    public void testObjectGenericRecordConversion() {
        TestAvroConversion obj = createObjectGenericRecordConversion();
        List<GenericRecord> records = AvroUtils.objectsToGenericRecords(TestAvroConversion.class,
                Arrays.asList(obj));

        // convert generic record back to object
        TestAvroConversion converted = AvroUtils.genericRecordToObject(records.get(0), TestAvroConversion.class);

        validateObjectGenericRecordConversion(obj, converted);
    }

    private TestAvroConversion createObjectGenericRecordConversion() {
        TestAvroConversion obj = new TestAvroConversion();
        obj.shortAttr = (short) 1;
        obj.intAttr = 2;
        obj.intWrapAttr = 3;
        obj.longAttr = 4L;
        obj.longWrapAttr = 5L;
        obj.floatAttr = (float) 6.0;
        obj.floatWrapAttr = (float) 7.0;
        obj.doubleAttr = 8.0;
        obj.doubleWrapAttr = 9.0;
        obj.boolAttr = true;
        obj.boolWrapAttr = Boolean.TRUE;
        obj.strAttr = "ABC";
        obj.enumAttr = TestAvroConversionEnum.ENUM1;
        return obj;
    }

    private void validateObjectGenericRecordConversion(TestAvroConversion origin,
            TestAvroConversion converted) {
        Assert.assertEquals(converted.shortAttr, origin.shortAttr);
        Assert.assertEquals(converted.intAttr, origin.intAttr);
        Assert.assertEquals(converted.intWrapAttr, origin.intWrapAttr);
        Assert.assertEquals(converted.longAttr, origin.longAttr);
        Assert.assertEquals(converted.longWrapAttr, origin.longWrapAttr);
        Assert.assertEquals(converted.floatAttr, origin.floatAttr);
        Assert.assertEquals(converted.floatWrapAttr, origin.floatWrapAttr);
        Assert.assertEquals(converted.doubleAttr, origin.doubleAttr);
        Assert.assertEquals(converted.doubleWrapAttr, origin.doubleWrapAttr);
        Assert.assertEquals(converted.boolAttr, origin.boolAttr);
        Assert.assertEquals(converted.boolWrapAttr, origin.boolWrapAttr);
        Assert.assertEquals(converted.strAttr, origin.strAttr);
        Assert.assertEquals(converted.nullAttr, origin.nullAttr);
        Assert.assertEquals(converted.enumAttr, origin.enumAttr);
    }

    static class TestAvroConversion {
        @SuppressWarnings("unused")
        public static final String STATIC_ATTR = "STATIC_ATTR";

        private short shortAttr;
        private int intAttr;
        private Integer intWrapAttr;
        private long longAttr;
        private Long longWrapAttr;
        private float floatAttr;
        private Float floatWrapAttr;
        private double doubleAttr;
        private Double doubleWrapAttr;
        private boolean boolAttr;
        private Boolean boolWrapAttr;
        private String strAttr;
        private String nullAttr; // don't set anything to test null
        private TestAvroConversionEnum enumAttr;
    }

    // to test that exact enum identifier should be written to generic record
    // instead of display name
    enum TestAvroConversionEnum {
        ENUM1("Enum1"), ENUM2("Enum2");

        private final String name;

        TestAvroConversionEnum(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
