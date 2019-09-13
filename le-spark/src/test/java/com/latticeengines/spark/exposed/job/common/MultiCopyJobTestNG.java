package com.latticeengines.spark.exposed.job.common;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.spark.common.MultiCopyConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MultiCopyJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        CopyConfig copy1 = new CopyConfig();
        copy1.setSelectAttrs(Arrays.asList("Id", "Attr1", "Attr2", "Attr4"));
        copy1.setDropAttrs(Arrays.asList("Attr2", "Attr3"));
        copy1.setRenameAttrs(ImmutableMap.of("Attr1", "newAttr1"));

        CopyConfig copy2 = new CopyConfig();
        copy2.setSelectAttrs(Arrays.asList("Id", "Attr1", "Attr3"));
        copy2.setDropAttrs(Arrays.asList("Attr2", "Attr4"));

        MultiCopyConfig config = new MultiCopyConfig();
        config.setCopyConfigs(Arrays.asList(copy1, copy2));
        config.setSpecialTarget(1, DataUnit.DataFormat.PARQUET);

        SparkJobResult result = runSparkJob(MultiCopyJob.class, config);
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
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifyOutput1, this::verifyOutput2);
    }

    private Boolean verifyOutput1(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 2);
            Assert.assertNotNull(record.getSchema().getField("Id"), record.toString());
            Assert.assertNotNull(record.getSchema().getField("newAttr1"), record.toString());
        });
        return true;
    }

    private Boolean verifyOutput2(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 3);
            Assert.assertNotNull(record.getSchema().getField("Id"), record.toString());
        });
        return true;
    }

}
