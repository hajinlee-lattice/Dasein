package com.latticeengines.spark.exposed.job.common;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ConcatJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        CopyConfig config = new CopyConfig();
        SparkJobResult result = runSparkJob(CopyJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr4", Boolean.class) //
        );
        Object[][] data = new Object[][] { //
                {1, "1", 1L, true}, //
                {2, "2", null, true}, //
                {3, null, 3L, false}, //
                {4, "4", 4L, true}, //
        };
        uploadHdfsDataUnit(data, fields);

        fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr3", Boolean.class) //
        );
        data = new Object[][] { //
                {2, "22", null, true}, //
                {3, "23", -3L, false}, //
                {4, "24", null, null}, //
                {5, "25", -5L, false} //
        };
        uploadHdfsDataUnit(data, fields);

        fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr5", String.class)
        );
        data = new Object[][] { //
                {1, "51"}, //
                {2, "52"}, //
                {3, "53"} //
        };
        uploadHdfsDataUnit(data, fields);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 6);
        });
        return true;
    }

}
