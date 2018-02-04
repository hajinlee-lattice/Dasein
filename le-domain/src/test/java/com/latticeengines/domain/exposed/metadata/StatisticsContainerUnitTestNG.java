package com.latticeengines.domain.exposed.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;

public class StatisticsContainerUnitTestNG {

    private static final String RESOURCE_ROOT = "com/latticeengines/domain/exposed/util/statsCubeUtilsUnitTestNG/";

    @Test(groups = "unit")
    public void testKryoSerDe() throws Exception {
        InputStream is = readResource("statscubes.json.gz");
        GZIPInputStream gis = new GZIPInputStream(is);
        Map<String, StatsCube> cubes = JsonUtils.deserialize(gis, new TypeReference<Map<String, StatsCube>>() {
        });

        StatisticsContainer container = new StatisticsContainer();
        container.setName("StatsContainer");
        container.setVersion(DataCollection.Version.Blue);
        container.setStatsCubes(cubes);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, container);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        StatisticsContainer deserialized = KryoUtils.read(bis, StatisticsContainer.class);
        Assert.assertEquals(deserialized.getName(), "StatsContainer");
        Assert.assertEquals(deserialized.getVersion(), DataCollection.Version.Blue);
    }

    private InputStream readResource(String fileName) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE_ROOT + fileName);
    }

}
