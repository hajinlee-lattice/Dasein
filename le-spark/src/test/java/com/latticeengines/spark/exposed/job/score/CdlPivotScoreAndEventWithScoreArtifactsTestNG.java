package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.PivotScoreAndEventJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CdlPivotScoreAndEventWithScoreArtifactsTestNG extends SparkJobFunctionalTestNGBase {

    private PivotScoreAndEventJobConfig getStandardConfig() {
        PivotScoreAndEventJobConfig config = new PivotScoreAndEventJobConfig();
        String modelguid1 = "ms__71fa73d2-ce4b-483a-ab1a-02e4471cd0fc-RatingEn";
        String modelguid2 = "ms__af81bb1f-a71e-4b3a-89cd-3a9d0c02b0d1-CDLEnd2E";
        config.scoreFieldMap = ImmutableMap.of( //
                modelguid1, InterfaceName.ExpectedRevenue.name(), //
                modelguid2, InterfaceName.RawScore.name() //
        );

        String sd = loadScoreDerivation("/pivotScoreAndEvent/scorederivation_cdl_514_1.json");
        String fitFunctionParams = loadFitFunctionParameters("/pivotScoreAndEvent/fitfunctionparameters.json");
        config.scoreDerivationMap = ImmutableMap.of(modelguid1, sd, modelguid2, sd);
        config.fitFunctionParametersMap = ImmutableMap.of(modelguid1, fitFunctionParams, modelguid2, fitFunctionParams);
        return config;
    }

    @Override
    protected String getJobName() {
        return "pivotScoreAndEvent";
    }

    @Override
    protected String getScenarioName() {
        return "CDLScoreOutputWithScoreArtifacts";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("InputTable");
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        SparkJobResult result = runSparkJob(PivotScoreAndEventJob.class, getStandardConfig());
        verify(result, Collections.singletonList(this::verifyStandardParameters));

    }

    private Boolean verifyStandardParameters(HdfsDataUnit tgt) {
        List<GenericRecord> outputRecords = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            outputRecords.add(record);
        });
        assertEquals(outputRecords.size(), 69);
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
