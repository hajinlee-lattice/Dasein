package com.latticeengines.spark.exposed.job.common;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CopyTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        CopyConfig config = new CopyConfig();
        config.setSelectAttrs(Arrays.asList("Id", "Attr1", "Attr2", "Attr4"));
        config.setDropAttrs(Arrays.asList("Attr2", "Attr3"));
        config.setRenameAttrs(ImmutableMap.of("Attr1", "newAttr1"));
        config.setAddTimestampAttrs(true);
        SparkJobResult result = runSparkJob(CopyJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr3", Integer.class) //
        );
        Object[][] data = new Object[][] { //
                {1, "1", 1L, 1}, //
                {2, "2", null, 2}, //
                {3, null, 3L, 3}, //
                {4, "4", 4L, 4}, //
        };
        uploadHdfsDataUnit(data, fields);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
//            System.out.println(record);
            Assert.assertNotNull(record.getSchema().getField("Id"), record.toString());
            Assert.assertNotNull(record.getSchema().getField("newAttr1"), record.toString());
            Assert.assertNotNull(record.getSchema().getField(InterfaceName.CDLCreatedTime.name()), record.toString());
            Assert.assertNotNull(record.getSchema().getField(InterfaceName.CDLUpdatedTime.name()), record.toString());
            Assert.assertEquals(record.getSchema().getFields().size(), 4);
        });
        return true;
    }

}
