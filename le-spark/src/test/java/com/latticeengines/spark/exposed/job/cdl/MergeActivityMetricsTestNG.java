package com.latticeengines.spark.exposed.job.cdl;


import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ActivityStoreSparkIOMetadata;
import com.latticeengines.domain.exposed.spark.cdl.MergeActivityMetricsJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeActivityMetricsTestNG extends SparkJobFunctionalTestNGBase {
    private static final String AccountId = InterfaceName.AccountId.name();
    private static final String M1A1 = "M1A1"; // metrics 1 dimension 1
    private static final String M1A2 = "M1A2"; // metrics 1 dimension 2
    private static final String M2A1 = "M2A1"; // metrics 2 dimension 1
    private static final String M2A2 = "M2A2"; // metrics 2 dimension 2

    private static final String ACCOUNT_ENTITY = BusinessEntity.Account.name();
    private static final String SERVING_ENTITY = TableRoleInCollection.WebVisitProfile.name();

    private static List<Pair<String, Class<?>>> FIELDS_M1;
    private static List<Pair<String, Class<?>>> FIELDS_M2;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        createDataFramesToMerge();
    }

    @Test(groups = "functional")
    public void test() {
        String mergedTableLabel = String.format("%s_%s", ACCOUNT_ENTITY, SERVING_ENTITY);
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        ActivityStoreSparkIOMetadata.Details details = new ActivityStoreSparkIOMetadata.Details();
        details.setStartIdx(0);
        details.setLabels(Arrays.asList("someGroup1", "someGroup2"));
        inputMetadata.setMetadata(Collections.singletonMap(mergedTableLabel, details));

        MergeActivityMetricsJobConfig config = new MergeActivityMetricsJobConfig();
        config.mergedTableLabels = Collections.singletonList(mergedTableLabel);
        config.inputMetadata = inputMetadata;

        SparkJobResult result = runSparkJob(MergeActivityMetrics.class, config);
        System.out.println(result.getOutput());
        Assert.assertFalse(StringUtils.isEmpty(result.getOutput()));
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(outputMetadata.getMetadata());
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        verify(result, Collections.singletonList(this::verifyMerged));
    }

    private void createDataFramesToMerge() {
        FIELDS_M1 = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(M1A1, String.class), //
                Pair.of(M1A2, String.class)
        );
        Object[][] data1 = new Object[][]{ //
                {"acc1", "m1a1row1", "m1A2row1"}, //
                {"acc2", "m1a1row2", "m1A2row2"}
        };
        FIELDS_M2 = Arrays.asList( //
                Pair.of(AccountId, String.class), //
                Pair.of(M2A1, String.class), //
                Pair.of(M2A2, String.class) //
        );
        Object[][] data2 = new Object[][]{ //
                {"acc2", "m2a1row1", "m2A2row1"}, //
                {"acc3", "m2a1row2", "m2A2row2"}
        };
        uploadHdfsDataUnit(data1, FIELDS_M1);
        uploadHdfsDataUnit(data2, FIELDS_M2);
    }

    private Boolean verifyMerged(HdfsDataUnit merged) {
        Iterator<GenericRecord> iterator = verifyAndReadTarget(merged);
        int rowCount = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            Assert.assertEquals(record.getSchema().getFields().size(), FIELDS_M1.size() + FIELDS_M2.size() - 1);
            rowCount++;
        }
        Assert.assertEquals(rowCount, 3);
        return false;
    }
}
