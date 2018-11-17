package com.latticeengines.scoring.dataflow;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class PivotScoreAndEventTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PivotScoreAndEventTestNG.class);

    private PivotScoreAndEventParameters getStandardParameters() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("InputTable");
        String modelguid = "ms__f7f1eb16-0d26-4aa1-8c4a-3ac696e13d06-PLS_model";
        params.setAvgScores(ImmutableMap.of(modelguid, 0.05));
        params.setScoreFieldMap(ImmutableMap.of(modelguid, InterfaceName.Event.name()));
        return params;
    }

    @Override
    protected String getScenarioName() {
        return "ScoreOutput";
    }

    @Override
    protected String getFlowBeanName() {
        return "pivotScoreAndEvent";
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        executeDataFlow(getStandardParameters());
        List<GenericRecord> outputRecords = readOutput();
        Assert.assertEquals(outputRecords.size(), 17);

        CSVFormat format = LECSVFormat.format.withSkipHeaderRecord(true);
        try (CSVParser parser = new CSVParser(new InputStreamReader(
                ClassLoader.getSystemResourceAsStream(String.format("%s/%s/pivot_score_event_result.csv", //
                        getFlowBeanName(), getScenarioName()))),
                format)) {
            List<CSVRecord> csvRecords = parser.getRecords();

            for (int i = 0; i < outputRecords.size(); i++) {
                CSVRecord csvRecord = csvRecords.get(i);
                GenericRecord avroRecord = outputRecords.get(i);
                log.info("avro:" + avroRecord);
                log.info("csv:" + csvRecord);
                for (String field: Arrays.asList("Score", "TotalEvents", "TotalPositiveEvents", "Lift")) {
                    Assert.assertEquals(Double.valueOf(csvRecord.get(field)), Double.valueOf(avroRecord.get(field).toString()));
                }
            }
        }
    }

}
