package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeCSVConfig;
import com.latticeengines.spark.exposed.job.common.CSVJobBaseTestNG;


public class MergeCSVTestNG extends CSVJobBaseTestNG {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        MergeCSVConfig config = new MergeCSVConfig();
        config.setCompress(true);
        config.setDisplayNames(ImmutableMap.of( //
                "Attr1", "My Attr 1", //
                "Attr3", "My Attr 3" //
        ));
        config.setDateAttrsFmt(ImmutableMap.of( //
                "Attr2", FMT_1, //
                "Attr3", FMT_2
        ));
        SparkJobResult result = runSparkJob(MergeCSVJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        //  Attr1 --> Name, Attr2 --> Date, Attr3 --> Time stamp
        String[] headers = Arrays.asList("Id", "Attr1", "Attr2", "Attr3").toArray(new String[]{});
        Object[][] fields = new Object[][]{ //
                {1, "1", now, now}, //
                {2, "2", null, now}, //
                {3, null, now, now}, //
                {4, "4", now, null}, //
                {5, "Hello world, \"Aloha\", yeah?", now, now}, //
        };
        uploadHdfsDataUnitWithCSVFmt(headers, fields);
    }

    public void verifyHeaders(Map<String, Integer> headerMap) {
        Assert.assertEquals(headerMap.get("Id"), Integer.valueOf(0));
        Assert.assertEquals(headerMap.get("My Attr 1"), Integer.valueOf(1));
        Assert.assertEquals(headerMap.get("Attr2"), Integer.valueOf(2));
        Assert.assertEquals(headerMap.get("My Attr 3"), Integer.valueOf(3));
    }

}
