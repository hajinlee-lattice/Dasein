package com.latticeengines.spark.exposed.job.dcp;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchedDuns;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.RollupDataReportConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class RollupDataReportJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final List<Pair<String, Class<?>>> FIELDS = Arrays.asList(
            Pair.of(MatchedDuns, String.class),
            Pair.of("cnt", Integer.class)
    );

    @Test(groups = "functional")
    public void testDataReportTree() {
        String input1 = uploadData();
        String input2 = uploadData();
        String input3 = uploadData();
        RollupDataReportConfig config = new RollupDataReportConfig();
        config.setMatchedDunsAttr(MatchedDuns);
        config.setInputOwnerIdToIndex(ImmutableMap.of("uploadId1", 0, "uploadId2", 1, "sourceId2", 2));

        List<String> ownerIds = ImmutableList.of("sourceId1", "projectId1");
        config.setUpdatedOwnerIds(ownerIds);
        config.setNumTargets(ownerIds.size());
        config.setMode(DataReportMode.UPDATE);

        config.setParentIdToChildren(ImmutableMap.of("sourceId1", ImmutableSet.of("uploadId1", "uploadId2"), "projectId1",
                ImmutableSet.of("sourceId1", "sourceId2")));
        SparkJobResult result = runSparkJob(RollupDataReportJob.class, config, Arrays.asList(input1, input2, input3),
                getWorkspace());
        verify(result, Arrays.asList(this::verifySingleTarget, this::verifySingleTarget));

    }

    @Test(groups = "functional")
    public void testException() {
        RollupDataReportConfig config = new RollupDataReportConfig();
        config.setMatchedDunsAttr(MatchedDuns);
        config.setInputOwnerIdToIndex(new HashMap<>());

        List<String> ownerIds = ImmutableList.of("sourceId1", "projectId1");
        config.setUpdatedOwnerIds(ownerIds);
        config.setNumTargets(ownerIds.size());
        config.setMode(DataReportMode.UPDATE);

        config.setParentIdToChildren(ImmutableMap.of("sourceId1", ImmutableSet.of(), "projectId1",
                ImmutableSet.of("sourceId1")));
        boolean hasError = false;
        try {
            runSparkJob(RollupDataReportJob.class, config, Collections.emptyList(),
                    getWorkspace());
        } catch (Exception e) {
            hasError = true;
        }
        Assert.assertTrue(hasError);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifySingleTarget, this::verifySingleTarget);
    }


    @Override
    public Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
        });
        return true;
    }

    @Override
    protected void verifyOutput(String output) {
        List<DataReport.DuplicationReport> dupReports = JsonUtils.convertList(JsonUtils.deserialize(output,
                List.class), DataReport.DuplicationReport.class);
        Assert.assertEquals(dupReports.size(), 2);
        DataReport.DuplicationReport dupReport1 = dupReports.get(0);
        DataReport.DuplicationReport dupReport2 = dupReports.get(1);
        Assert.assertEquals(dupReport1.getUniqueRecords(), Long.valueOf(0L));
        Assert.assertEquals(dupReport1.getDistinctRecords(), Long.valueOf(5));
        Assert.assertEquals(dupReport1.getDuplicateRecords(), Long.valueOf(30));

        Assert.assertEquals(dupReport2.getUniqueRecords(), Long.valueOf(0L));
        Assert.assertEquals(dupReport2.getDistinctRecords(), Long.valueOf(5));
        Assert.assertEquals(dupReport2.getDuplicateRecords(), Long.valueOf(45));

    }

    private String uploadData() {
        Object[][] data = new Object[][] {
                { "123456", 1},
                { "234567", 2},
                { "345678", 3},
                { "456789", 4},
                { "567890", 5},
        };
        return uploadHdfsDataUnit(data, FIELDS);
    }

}
