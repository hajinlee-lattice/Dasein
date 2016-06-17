package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AvroUtilsUnitTestNG {

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

    @SuppressWarnings("deprecation")
    @Test(groups = "unit", dataProvider = "avscFileProvider", enabled = true)
    public void generateHiveCreateTableStatement(String avscFileName) throws Exception {
        URL url = ClassLoader.getSystemResource(String.format(
                "com/latticeengines/common/exposed/util/avroUtilsData/%s", avscFileName));
        File avscFile = new File(url.getFile());
        Schema schema = Schema.parse(avscFile);
        String hiveTableDDL = AvroUtils.generateHiveCreateTableStatement("ABC", "/tmp/Stoplist", schema);
        System.out.println(hiveTableDDL);
    }


    @Test(groups = "unit")
    public void testAlignFields() throws Exception {
        Schema schema1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Shuffled\",\"doc\":\"Testing data\","
                + "\"fields\":[" //
                + "{\"name\":\"Field2\",\"type\":\"int\"}," //
                + "{\"name\":\"Field1\",\"type\":[\"int\",\"null\"]}]}");
        Schema schema2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Ordered\",\"doc\":\"Testing data\","
                + "\"fields\":[" //
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
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\","
                + "\"fields\":[" + "{\"name\":\"Field1\",\"type\":\"int\"},"
                + "{\"name\":\"Field2\",\"type\":[\"int\",\"null\"]}]}");
        for (Schema.Field field: schema.getFields()) {
            Assert.assertEquals(AvroUtils.getType(field), Schema.Type.INT);
        }

    }

    @DataProvider(name = "avscFileProvider")
    public Object[][] getAvscFile() {
        return new Object[][] { { "aps.avsc" }, //
                { "leaccount.avsc" }, //
                { "leaccountextensions.avsc" } };
    }

}
