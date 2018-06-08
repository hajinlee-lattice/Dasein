package com.latticeengines.scoring.dataflow;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoringapi.FitFunctionParameters;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

import static com.latticeengines.domain.exposed.scoringapi.Model.HDFS_SCORE_ARTIFACT_BASE_DIR;
import static org.testng.Assert.assertEquals;

@ContextConfiguration(locations = {"classpath:serviceflows-scoring-dataflow-context.xml"})
public class CdlPivotScoreAndEventWithScoreArtifactsTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private PivotScoreAndEventParameters getStandardParameters() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("InputTable");
        String modelguid1 = "ms__71fa73d2-ce4b-483a-ab1a-02e4471cd0fc-RatingEn";
        String modelguid2 = "ms__af81bb1f-a71e-4b3a-89cd-3a9d0c02b0d1-CDLEnd2E";
        params.setScoreFieldMap(ImmutableMap.of( //
                                                 modelguid1, InterfaceName.ExpectedRevenue.name(), //
                                                 modelguid2, InterfaceName.RawScore.name() //
        ));

        String sd = loadScoreDerivation("/pivotScoreAndEvent/scorederivation_cdl_514_1.json");
        String fitFunctionParams =
            loadFitFunctionParameters("/pivotScoreAndEvent/fitfunctionparameters.json");
        params.setScoreDerivationMap(
            ImmutableMap.of(modelguid1, sd, modelguid2, sd));
        params.setFitFunctionParametersMap(
            ImmutableMap.of(modelguid1, fitFunctionParams, modelguid2, fitFunctionParams));
        return params;
    }

    @Override
    protected String getFlowBeanName() {
        return "pivotScoreAndEvent";
    }

    @Override
    protected String getScenarioName() {
        return "CDLScoreOutputWithScoreArtifacts";
    }

    @Override
    protected Map<String, Table> getSources() {
        URL url = ClassLoader.getSystemResource("pivotScoreAndEvent/CDLScoreOutputWithScoreArtifacts/InputTable");
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        Table t = MetadataConverter.getTable(config, url.getPath() + "/score_data.avro");
        Extract e = new Extract();
        e.setName("ScoreResult");
        e.setPath(url.getPath());
        t.setExtracts(Arrays.asList(e));
        return ImmutableMap.of("InputTable", t);
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        executeDataFlow(getStandardParameters());
        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 69);
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
