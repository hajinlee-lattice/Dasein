package com.latticeengines.leadprioritization.dataflow;

import static org.testng.Assert.assertEquals;

import java.io.InputStreamReader;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-dataflow-context.xml" })
public class PivotScoreAndEventTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private PivotScoreAndEventParameters getStandardParameters() {
        PivotScoreAndEventParameters params = new PivotScoreAndEventParameters("ScoreOutput", 0.05);
        return params;
    }

    @Override
    protected String getFlowBeanName() {
        return "pivotScoreAndEvent";
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        executeDataFlow(getStandardParameters());
        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 17);

        CSVFormat format = LECSVFormat.format.withSkipHeaderRecord(true);
        try (CSVParser parser = new CSVParser(new InputStreamReader(
                ClassLoader.getSystemResourceAsStream(String.format("%s/%s/pivot_score_event_result.csv", //
                        getFlowBeanName(), getStandardParameters().getScoreOutputTableName()))),
                format)) {
            List<CSVRecord> csvRecords = parser.getRecords();

            for (int i = 0; i < outputRecords.size(); i++) {
                CSVRecord csvRecord = csvRecords.get(i);
                GenericRecord avroRecord = outputRecords.get(i);
                for (int j = 0; j < csvRecord.size(); j++) {
                    assertEquals(Double.valueOf(csvRecord.get(j)) - Double.valueOf(avroRecord.get(j).toString()), 0.0);
                }
            }
        }
    }
}
