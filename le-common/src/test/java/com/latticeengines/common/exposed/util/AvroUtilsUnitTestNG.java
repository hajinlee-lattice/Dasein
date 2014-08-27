package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;

import org.apache.avro.Schema;
import org.testng.annotations.Test;

public class AvroUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testCombineSchemas() throws Exception {

        URL url1 = ClassLoader.getSystemResource("com/latticeengines/common/exposed/util/avroUtilsData/schema1.avsc");
        File avroFile1 = new File(url1.getFile());
        URL url2 = ClassLoader.getSystemResource("com/latticeengines/common/exposed/util/avroUtilsData/schema2.avsc");
        File avroFile2 = new File(url2.getFile());
        Schema s1 = Schema.parse(avroFile1);
        Schema s2 = Schema.parse(avroFile2);

        
        Object[] combinedSchema = AvroUtils.combineSchemas(s1, s2);

        assertEquals(((Schema) combinedSchema[0]).getFields().size(), s1.getFields().size()
                + s2.getFields().size());
    }
}
