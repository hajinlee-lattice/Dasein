package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;

public class StatsCubeUtilsUnitTestNG {

    private static final String RESOURCE_ROOT = "com/latticeengines/domain/exposed/util/statsCubeUtilsUnitTestNG/";

    @Test(groups = "unit")
    public void testParseAvro() throws Exception {
        Iterator<GenericRecord> records = readAvro();
        StatsCube cube = StatsCubeUtils.parseAvro(records);
        Assert.assertNotNull(cube);
        ObjectMapper om = new ObjectMapper();
        System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(cube));
    }


    private Iterator<GenericRecord> readAvro() throws IOException {
        InputStream avroIs = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(RESOURCE_ROOT + "amstats.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(avroIs);
        return records.subList(0, 10).iterator();
    }


}
