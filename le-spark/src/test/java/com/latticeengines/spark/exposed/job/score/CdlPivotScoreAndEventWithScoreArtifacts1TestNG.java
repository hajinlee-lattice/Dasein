package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.PivotScoreAndEventJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CdlPivotScoreAndEventWithScoreArtifacts1TestNG extends SparkJobFunctionalTestNGBase {

    private PivotScoreAndEventJobConfig getStandardConfig() {
        PivotScoreAndEventJobConfig config = new PivotScoreAndEventJobConfig();
        String modelguid1 = "ms__25630e76-89ee-4fbe-b4bf-24182d9d67d5-ai_ba6vq";
        config.scoreFieldMap = ImmutableMap.of( //
                modelguid1, InterfaceName.RawScore.name() //
        );

        String sd = loadScoreDerivation("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/scorederivation.json");
        String fitFunctionParams = loadFitFunctionParameters(
                "/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/fitfunctionparameters.json");
        config.scoreDerivationMap = ImmutableMap.of(modelguid1, sd);
        config.fitFunctionParametersMap = ImmutableMap.of(modelguid1, fitFunctionParams);
        return config;
    }

    private PivotScoreAndEventJobConfig getBadConfig() {
        PivotScoreAndEventJobConfig config = new PivotScoreAndEventJobConfig();
        String modelguid1 = "ms__25630e76-89ee-4fbe-b4bf-24182d9d67d5-ai_ba6vq";
        config.scoreFieldMap = ImmutableMap.of( //
                modelguid1, InterfaceName.RawScore.name() //
        );

        String sd = loadScoreDerivation("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/scorederivation.json");
        String fitFunctionParams = loadFitFunctionParameters(
                "/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/fitfunctionparameters_bad.json");
        config.scoreDerivationMap = ImmutableMap.of(modelguid1, sd);
        config.fitFunctionParametersMap = ImmutableMap.of(modelguid1, fitFunctionParams);
        return config;
    }

    private PivotScoreAndEventJobConfig getStandardConfig2() {
        PivotScoreAndEventJobConfig config = new PivotScoreAndEventJobConfig();
        String modelguid1 = "ms__25630e76-89ee-4fbe-b4bf-24182d9d67d5-ai_ba6vq";
        config.scoreFieldMap = ImmutableMap.of( //
                modelguid1, InterfaceName.RawScore.name() //
        );

        String sd = loadScoreDerivation("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/scorederivation.json");
        String fitFunctionParams = loadFitFunctionParameters(
                "/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/fitfunctionparameters2.json");
        config.scoreDerivationMap = ImmutableMap.of(modelguid1, sd);
        config.fitFunctionParametersMap = ImmutableMap.of(modelguid1, fitFunctionParams);
        return config;
    }

    @Override
    protected String getJobName() {
        return "pivotScoreAndEvent";
    }

    @Override
    protected String getScenarioName() {
        return "CDLScoreOutputWithScoreArtifacts1";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("InputTable");
    }

    @Test(groups = "functional")
    public void testStandardParameters() throws Exception {
        SparkJobResult result = runSparkJob(PivotScoreAndEventJob.class, getStandardConfig());
        verify(result, Collections.singletonList(this::verifyStandardParameters));
    }

    private Boolean verifyStandardParameters(HdfsDataUnit tgt) {
        List<GenericRecord> outputRecords = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            outputRecords.add(record);
        });
        Collections.sort(outputRecords, (GenericRecord g1, GenericRecord g2) -> Double
                .compare(Double.valueOf(g2.get("Score").toString()), Double.valueOf(g1.get("Score").toString())));
        assertEquals(outputRecords.size(), 95);
        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
                .generateBucketedScoreSummary(outputRecords);
        System.out.println(bucketedScoreSummary);
        Assert.assertEquals(7733, bucketedScoreSummary.getTotalNumLeads());
        return true;
    }

    @Test(groups = "functional")
    public void testBadParameters() throws Throwable {
        try {
            SparkJobResult result = runSparkJob(PivotScoreAndEventJob.class, getBadConfig());
            Assert.fail("Should not come here");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getCause().getMessage().contains("Invalid fit function parameter beta: Infinity"));
        }
    }

    @Test(groups = "functional")
    public void testStandardParameters2() throws Exception {
        SparkJobResult result = runSparkJob(PivotScoreAndEventJob.class, getStandardConfig2());
        verify(result, Collections.singletonList(this::verifyStandardParameters2));

    }

    private Boolean verifyStandardParameters2(HdfsDataUnit tgt) {
        List<GenericRecord> outputRecords = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            outputRecords.add(record);
        });
        Collections.sort(outputRecords, (GenericRecord g1, GenericRecord g2) -> Double
                .compare(Double.valueOf(g2.get("Score").toString()), Double.valueOf(g1.get("Score").toString())));
        assertEquals(outputRecords.size(), 95);
        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
                .generateBucketedScoreSummary(outputRecords);
        System.out.println(bucketedScoreSummary);
        Assert.assertEquals(7733, bucketedScoreSummary.getTotalNumLeads());

        return true;
    }

    @Test(groups = "functional")
    public void testStandardParameters3() throws Exception {
        SparkJobResult result = runSparkJob(PivotScoreAndEventJob.class, getStandardConfig());
        verify(result, Collections.singletonList(this::verifyStandardParameters3));
    }

    private Boolean verifyStandardParameters3(HdfsDataUnit tgt) {
        List<GenericRecord> outputRecords = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            outputRecords.add(record);
        });
        Collections.sort(outputRecords, (GenericRecord g1, GenericRecord g2) -> Double
                .compare(Double.valueOf(g2.get("Score").toString()), Double.valueOf(g1.get("Score").toString())));
        assertEquals(outputRecords.size(), 95);
        GenericRecord nullRecord = outputRecords.get(0);
        nullRecord.put("Score", null);

        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
                .generateBucketedScoreSummary(outputRecords);
        System.out.println(bucketedScoreSummary);
        Assert.assertEquals(7733, bucketedScoreSummary.getTotalNumLeads());
        return true;
    }

    private String loadScoreDerivation(String resourceName) {
        try {
            InputStream inputStream = ClassLoader.class.getResourceAsStream(resourceName);
            ScoreDerivation sd = JsonUtils.deserialize(inputStream, ScoreDerivation.class);
            return JsonUtils.serialize(sd);
        } catch (Exception e) {
            throw new RuntimeException("Cannot load resource " + resourceName);
        }
    }

    private String loadFitFunctionParameters(String resourceName) {
        try {
            InputStream inputStream = ClassLoader.class.getResourceAsStream(resourceName);
            FitFunctionParameters params = JsonUtils.deserialize(inputStream, FitFunctionParameters.class);
            return JsonUtils.serialize(params);
        } catch (Exception e) {
            throw new RuntimeException("Cannot load resource " + resourceName);
        }
    }

}
