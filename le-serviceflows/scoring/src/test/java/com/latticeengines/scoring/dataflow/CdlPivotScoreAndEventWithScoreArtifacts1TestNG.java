package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.util.BucketedScoreSummaryUtils;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = {"classpath:serviceflows-scoring-dataflow-context.xml"})
public class CdlPivotScoreAndEventWithScoreArtifacts1TestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private PivotScoreAndEventParameters getStandardParameters() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("InputTable");
        String modelguid1 = "ms__25630e76-89ee-4fbe-b4bf-24182d9d67d5-ai_ba6vq";
        params.setScoreFieldMap(ImmutableMap.of( //
                                                 modelguid1, InterfaceName.RawScore.name() //
        ));

        String sd = loadScoreDerivation("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/scorederivation.json");
        String fitFunctionParams =
            loadFitFunctionParameters("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/fitfunctionparameters.json");
        params.setScoreDerivationMap(
            ImmutableMap.of(modelguid1, sd));
        params.setFitFunctionParametersMap(
            ImmutableMap.of(modelguid1, fitFunctionParams));
        return params;
    }

    private PivotScoreAndEventParameters getBadParameters() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("InputTable");
        String modelguid1 = "ms__25630e76-89ee-4fbe-b4bf-24182d9d67d5-ai_ba6vq";
        params.setScoreFieldMap(ImmutableMap.of( //
                                                 modelguid1, InterfaceName.RawScore.name() //
        ));

        String sd = loadScoreDerivation("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/scorederivation.json");
        String fitFunctionParams =
            loadFitFunctionParameters("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/fitfunctionparameters_bad.json");
        params.setScoreDerivationMap(
            ImmutableMap.of(modelguid1, sd));
        params.setFitFunctionParametersMap(
            ImmutableMap.of(modelguid1, fitFunctionParams));
        return params;
    }

    private PivotScoreAndEventParameters getStandardParameters2() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("InputTable");
        String modelguid1 = "ms__25630e76-89ee-4fbe-b4bf-24182d9d67d5-ai_ba6vq";
        params.setScoreFieldMap(ImmutableMap.of( //
                                                 modelguid1, InterfaceName.RawScore.name() //
        ));

        String sd = loadScoreDerivation("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/scorederivation.json");
        String fitFunctionParams =
            loadFitFunctionParameters("/pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/fitfunctionparameters2.json");
        params.setScoreDerivationMap(
            ImmutableMap.of(modelguid1, sd));
        params.setFitFunctionParametersMap(
            ImmutableMap.of(modelguid1, fitFunctionParams));
        return params;
    }

    @Override
    protected String getFlowBeanName() {
        return "pivotScoreAndEvent";
    }

    @Override
    protected String getScenarioName() {
        return "CDLScoreOutputWithScoreArtifacts1";
    }

    @Override
    protected Map<String, Table> getSources() {
        URL url = ClassLoader.getSystemResource("pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts1/InputTable");
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        Table t = MetadataConverter.getTable(config, url.getPath() + "/part-v006-o000-00000.avro");
        Extract e = new Extract();
        e.setName("ScoreResult");
        e.setPath(url.getPath());
        t.setExtracts(Arrays.asList(e));
        return ImmutableMap.of("InputTable", t);
    }

    @Test(groups = "functional")
    public void testStandardParameters() throws Exception {
        executeDataFlow(getStandardParameters());
        List<GenericRecord> outputRecords = readOutput();
        // reverse the output list to test we sort it later
        Collections.sort(
            outputRecords,
            (GenericRecord g1, GenericRecord g2) ->
                Double.compare(Double.valueOf(g2.get("Score").toString()), Double.valueOf(g1.get("Score").toString()))
        );
        assertEquals(outputRecords.size(), 95);
        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
            .generateBucketedScoreSummary(outputRecords);
        System.out.println(bucketedScoreSummary);
        Assert.assertEquals(7733, bucketedScoreSummary.getTotalNumLeads());
    }

    @Test(groups = "functional", expectedExceptions = IllegalArgumentException.class)
    public void testBadParameters() throws Exception {
        executeDataFlow(getBadParameters());
    }

    @Test(groups = "functional")
    public void testStandardParameters2() throws Exception {
        executeDataFlow(getStandardParameters2());
        List<GenericRecord> outputRecords = readOutput();
        // reverse the output list to test we sort it later
        Collections.sort(
            outputRecords,
            (GenericRecord g1, GenericRecord g2) ->
                Double.compare(Double.valueOf(g2.get("Score").toString()), Double.valueOf(g1.get("Score").toString()))
        );
        assertEquals(outputRecords.size(), 95);
        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
            .generateBucketedScoreSummary(outputRecords);
        System.out.println(bucketedScoreSummary);
        Assert.assertEquals(7733, bucketedScoreSummary.getTotalNumLeads());
    }

    @Test(groups = "functional")
    public void testStandardParameters3() throws Exception {
        executeDataFlow(getStandardParameters());
        List<GenericRecord> outputRecords = readOutput();
        // reverse the output list to test we sort it later
        Collections.sort(
            outputRecords,
            (GenericRecord g1, GenericRecord g2) ->
                Double.compare(Double.valueOf(g2.get("Score").toString()), Double.valueOf(g1.get("Score").toString()))
        );
        assertEquals(outputRecords.size(), 95);
        GenericRecord nullRecord = outputRecords.get(0);
        nullRecord.put("Score", null);

        BucketedScoreSummary bucketedScoreSummary = BucketedScoreSummaryUtils
            .generateBucketedScoreSummary(outputRecords);
        System.out.println(bucketedScoreSummary);
        Assert.assertEquals(7733, bucketedScoreSummary.getTotalNumLeads());
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
