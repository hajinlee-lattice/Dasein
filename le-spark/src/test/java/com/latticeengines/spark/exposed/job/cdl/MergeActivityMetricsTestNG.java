package com.latticeengines.spark.exposed.job.cdl;


import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    private static final String M1A1 = "M1A1"; // metrics 1 attribute 1
    private static final String M1A2 = "M1A2"; // metrics 1 attribute 2
    private static final String M2A1 = "M2A1"; // metrics 2 attribute 1
    private static final String M2A2 = "M2A2"; // metrics 2 attribute 2
    private static final String M3A3 = "M3A3"; // active metrics attr that needs to be deprecated

    private static final String ACCOUNT_ENTITY = BusinessEntity.Account.name();
    private static final String SERVING_ENTITY = TableRoleInCollection.WebVisitProfile.name();

    private static List<Pair<String, Class<?>>> FIELDS_M1;
    private static List<Pair<String, Class<?>>> FIELDS_M2;
    private static List<Pair<String, Class<?>>> FIELDS_M3; // active metrics

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        setupData();
    }

    @Test(groups = "functional")
    public void test() {
        String mergedTableLabel = String.format("%s_%s", ACCOUNT_ENTITY, SERVING_ENTITY);
        ActivityStoreSparkIOMetadata inputMetadata = new ActivityStoreSparkIOMetadata();
        ActivityStoreSparkIOMetadata.Details newMetricsDetails = new ActivityStoreSparkIOMetadata.Details();
        newMetricsDetails.setStartIdx(0);
        newMetricsDetails.setLabels(Arrays.asList("someGroup1", "someGroup2"));
        ActivityStoreSparkIOMetadata.Details activeMetricsDetails = new ActivityStoreSparkIOMetadata.Details();
        activeMetricsDetails.setStartIdx(2);
        Map<String, ActivityStoreSparkIOMetadata.Details> metadata = new HashMap<>();
        metadata.put(mergedTableLabel, newMetricsDetails);
        metadata.put(SERVING_ENTITY, activeMetricsDetails);
        inputMetadata.setMetadata(metadata);

        MergeActivityMetricsJobConfig config = new MergeActivityMetricsJobConfig();
        config.mergedTableLabels = Collections.singletonList(mergedTableLabel);
        config.inputMetadata = inputMetadata;

        SparkJobResult result = runSparkJob(MergeActivityMetrics.class, config);
        Assert.assertFalse(StringUtils.isEmpty(result.getOutput()));
        ActivityStoreSparkIOMetadata outputMetadata = JsonUtils.deserialize(result.getOutput(), ActivityStoreSparkIOMetadata.class);
        Assert.assertNotNull(outputMetadata.getMetadata());
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        Assert.assertEquals( // verify attributes to be deprecated
                outputMetadata.getMetadata().get(mergedTableLabel).getLabels(),
                Collections.singletonList(M3A3)
        );
        verify(result, Collections.singletonList(this::verifyMerged));
    }

    private void setupData() {
        FIELDS_M1 = Arrays.asList(
                Pair.of(AccountId, String.class),
                Pair.of(M1A1, Integer.class),
                Pair.of(M1A2, Integer.class)
        );
        Object[][] import1 = new Object[][]{
                {"acc1", 111, 121},
                {"acc2", 112, 122}
        };
        FIELDS_M2 = Arrays.asList( //
                Pair.of(AccountId, String.class),
                Pair.of(M2A1, Integer.class),
                Pair.of(M2A2, Integer.class)
        );
        Object[][] import2 = new Object[][]{
                {"acc2", 211, 221},
                {"acc3", 212, 222}
        };
        FIELDS_M3 = Arrays.asList(
                Pair.of(AccountId, String.class),
                Pair.of(M3A3, Integer.class)
        );
        Object[][] activeMetrics = new Object[][]{
                {"acc1", 331},
                {"acc2", 332},
                {"acc3", 333}
        };
        uploadHdfsDataUnit(import1, FIELDS_M1);
        uploadHdfsDataUnit(import2, FIELDS_M2);
        uploadHdfsDataUnit(activeMetrics, FIELDS_M3);
    }

    private Boolean verifyMerged(HdfsDataUnit merged) {
        Iterator<GenericRecord> iterator = verifyAndReadTarget(merged);
        int rowCount = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            Assert.assertEquals(record.getSchema().getFields().size(), FIELDS_M1.size() + FIELDS_M2.size() + FIELDS_M3.size() - 2);
            rowCount++;
        }
        Assert.assertEquals(rowCount, 3);
        return false;
    }
}
