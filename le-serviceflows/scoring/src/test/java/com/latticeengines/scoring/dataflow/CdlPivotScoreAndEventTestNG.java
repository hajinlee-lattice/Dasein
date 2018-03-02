package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class CdlPivotScoreAndEventTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CdlPivotScoreAndEventTestNG.class);

    private PivotScoreAndEventParameters getStandardParameters() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("InputTable");
        String modelguid1 = "ms__71fa73d2-ce4b-483a-ab1a-02e4471cd0fc-RatingEn";
        String modelguid2 = "ms__af81bb1f-a71e-4b3a-89cd-3a9d0c02b0d1-CDLEnd2E";
        params.setAvgScores(ImmutableMap.of(modelguid1, 0.05, modelguid2, 0.05));
        params.setExpectedValues(ImmutableMap.of(modelguid1, true, modelguid2, false));
        return params;
    }

    @Override
    protected Map<String, Table> getSources() {
        URL url = ClassLoader.getSystemResource("pivotScoreAndEvent/CDLScoreOutput/InputTable");
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        Table t = MetadataConverter.getTable(config, url.getPath() + "/score_data.avro");
        Extract e = new Extract();
        e.setName("ScoreResult");
        e.setPath(url.getPath());
        t.setExtracts(Arrays.asList(e));
        return ImmutableMap.of("InputTable", t);
    }

    @Override
    protected String getFlowBeanName() {
        return "pivotScoreAndEvent";
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        executeDataFlow(getStandardParameters());
        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 69);
    }
}
