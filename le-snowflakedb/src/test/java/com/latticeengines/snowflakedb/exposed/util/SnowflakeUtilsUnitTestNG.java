package com.latticeengines.snowflakedb.exposed.util;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.snowflakedb.util.SnowflakeUtils;

public class SnowflakeUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testSchemaToView() {
        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TestTable\",\"doc\":\"Testing data\","
                + "\"fields\":[" //
                + "{\"name\":\"Field1\",\"type\":\"int\"}," //
                + "{\"name\":\"Field2\",\"type\":\"string\"}," //
                + "{\"name\":\"Field3\",\"type\":[\"string\",\"null\"]}," //
                + "{\"name\":\"Field4\",\"type\":[\"int\",\"null\"]}]}");

        String view = SnowflakeUtils.schemaToView(schema);
        Assert.assertTrue(view.contains("RECORD:Field1::INT AS Field1"));
        Assert.assertTrue(view.contains("RECORD:Field2::VARCHAR AS Field2"));
        Assert.assertTrue(view.contains("RECORD:Field3::VARCHAR AS Field3"));
        Assert.assertTrue(view.contains("RECORD:Field4::INT AS Field4"));

        List<String> columnsToExpose = Arrays.asList("Field1", "field3");
        view = SnowflakeUtils.schemaToView(schema, columnsToExpose);
        Assert.assertTrue(view.contains("RECORD:Field1::INT AS Field1"));
        Assert.assertFalse(view.contains("RECORD:Field2::VARCHAR AS Field2"));
        Assert.assertTrue(view.contains("RECORD:Field3::VARCHAR AS Field3"));
        Assert.assertFalse(view.contains("RECORD:Field4::INT AS Field4"));
    }

}
