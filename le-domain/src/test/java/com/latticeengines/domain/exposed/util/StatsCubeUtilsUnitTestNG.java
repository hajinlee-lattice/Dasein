package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class StatsCubeUtilsUnitTestNG {

    private static final String RESOURCE_ROOT = "com/latticeengines/domain/exposed/util/statsCubeUtilsUnitTestNG/";

    @Test(groups = "unit")
    public void testParseAvro() throws Exception {
        Iterator<GenericRecord> records = readAvro();
        StatsCube cube = StatsCubeUtils.parseAvro(records);
        Assert.assertNotNull(cube);
        AttributeStats stats = cube.getStatistics().get("LatticeAccountId");
        long maxCount = stats.getNonNullCount();
        Assert.assertEquals(cube.getCount(), new Long(maxCount));

        cube.getStatistics().forEach((attrName, attrStats) -> {
            Assert.assertTrue(attrStats.getNonNullCount() <= maxCount, attrName + JsonUtils.pprint(attrStats));
        });
    }

    @Test(groups = "unit")
    public void testConstructStatsContainer() throws Exception {
        ObjectMapper om = new ObjectMapper();
        Map<BusinessEntity, List<ColumnMetadata>> cmMap = om.readValue(readResource("mdMap.json"),
                new TypeReference<Map<BusinessEntity, List<ColumnMetadata>>>() {
                });
        Map<BusinessEntity, StatsCube> cubeMap = om.readValue(readResource("cubeMap.json"),
                new TypeReference<Map<BusinessEntity, StatsCube>>() {
                });
        Statistics statistics = StatsCubeUtils.constructStatistics(cubeMap, cmMap);
        Assert.assertNotNull(statistics);
        System.out.println(JsonUtils.pprint(statistics));
    }

    private Iterator<GenericRecord> readAvro() throws IOException {
        InputStream avroIs = readResource("amstats.avro");
        List<GenericRecord> records = AvroUtils.readFromInputStream(avroIs);
        return records.iterator();
    }

    private InputStream readResource(String fileName) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE_ROOT + fileName);
    }

}
