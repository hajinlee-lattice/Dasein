package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.net.URL;

import org.apache.avro.Schema;
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

        assertEquals(((Schema) combinedSchema[0]).getFields().size(), s1.getFields().size()
                + s2.getFields().size());
        
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
    @Test(groups = "unit", dataProvider = "avscFileProvider")
    public void generateHiveCreateTableStatement(String avscFileName) throws Exception {
        URL url = ClassLoader.getSystemResource(String.format("com/latticeengines/common/exposed/util/avroUtilsData/%s", avscFileName));
        File avscFile = new File(url.getFile());
        Schema schema = Schema.parse(avscFile);
        String hiveTableDDL = AvroUtils.generateHiveCreateTableStatement(null, null, schema);
        System.out.println(hiveTableDDL);
    }
    
    @DataProvider(name = "avscFileProvider")
    public Object[][] getAvscFile() {
        return new Object[][] { 
          { "aps.avsc" }, //
          { "leaccount.avsc" }, //
          { "leaccountextensions.avsc" }
        };
    }
}
