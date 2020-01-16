package com.latticeengines.spark.exposed.job.common;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CSVJobConfigBase;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;

public class ConvertToCSVTestNG extends CSVJobBaseTestNG {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        ConvertToCSVConfig config = new ConvertToCSVConfig();
        config.setCompress(true);
        config.setDisplayNames(ImmutableMap.of( //
                "Attr1", "My Attr 1", //
                "Attr3", "My Attr 3" //
        ));
        config.setDateAttrsFmt(ImmutableMap.of( //
                "Attr2", FMT_1, //
                "Attr3", FMT_2, //
                EXPORT_TIME, CSVJobConfigBase.ISO_8601
        ));
        config.setExportTimeAttr(EXPORT_TIME);
        SparkJobResult result = runSparkJob(ConvertToCSVJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr3", Long.class) //
        );
        Object[][] data = new Object[][]{ //
                {1, "1", now, now}, //
                {2, "2", null, now}, //
                {3, null, now, now}, //
                {4, "4", now, null}, //
                {5, "Hello world, \"Aloha\", yeah?", now, now}, //
        };
        uploadHdfsDataUnit(data, fields);
    }

    public void verifyHeaders(Map<String, Integer> headerMap) {
        Assert.assertEquals(headerMap.get("Id"), Integer.valueOf(0));
        Assert.assertEquals(headerMap.get("My Attr 1"), Integer.valueOf(1));
        Assert.assertEquals(headerMap.get("Attr2"), Integer.valueOf(2));
        Assert.assertEquals(headerMap.get("My Attr 3"), Integer.valueOf(3));
        Assert.assertEquals(headerMap.get(EXPORT_TIME), Integer.valueOf(4));
    }
}
