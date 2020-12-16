package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MigrateActivityPartitionKeyJobConfig;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MigrateActivityPartitionKeyJobTestNG extends SparkJobFunctionalTestNGBase {

    private final String STREAM_ID = "stream_abc123";
    private final String AccountId = InterfaceName.AccountId.name();
    private final String __StreamDateId = InterfaceName.__StreamDateId.name();
    private final String StreamDateId = InterfaceName.StreamDateId.name();

    @Test(groups = "functional")
    public void test() {
        MigrateActivityPartitionKeyJobConfig config = new MigrateActivityPartitionKeyJobConfig();
        config.inputMetadata = new SparkIOMetadataWrapper();
        config.inputMetadata.setMetadata(constructInputMetadata());
        List<String> inputs = appendInput();
        SparkJobResult result = runSparkJob(MigrateActivityPartitionKeyJob.class, config, inputs, getWorkspace());
        System.out.println(result.getOutput());

        Assert.assertEquals(result.getTargets().size(), inputs.size());
        result.getTargets().forEach(table -> Assert.assertEquals(table.getPartitionKeys(), Collections.singletonList(StreamDateId)));
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
        Assert.assertEquals(outputMetadata.getMetadata().get(STREAM_ID).getLabels(), Arrays.asList(StreamDateId, StreamDateId));
    }

    private Map<String, SparkIOMetadataWrapper.Partition> constructInputMetadata() {
        Map<String, SparkIOMetadataWrapper.Partition> inputMetadata = new HashMap<>();
        SparkIOMetadataWrapper.Partition details = new SparkIOMetadataWrapper.Partition();
        details.setStartIdx(0);
        details.setLabels(Arrays.asList(__StreamDateId, __StreamDateId));
        inputMetadata.put(STREAM_ID, details);
        return inputMetadata;
    }

    private List<String> appendInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(AccountId, String.class),
                Pair.of(__StreamDateId, Integer.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", 1},
                {"acc2", 1},
                {"acc3", 2},
                {"acc4", 3},
                {"acc5", 5}
        };
        return Arrays.asList(uploadHdfsDataUnit(data, fields), uploadHdfsDataUnit(data, fields));
    }
}
