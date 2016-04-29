package com.latticeengines.leadprioritization.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-context.xml" })
public class CombineInputTableWithScoreTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private CombineInputTableWithScoreParameters getStandardParameters() {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters("ScoreResult",
                "InputTable");
        params.enableDebugging();
        return params;
    }

    @Override
    protected String getFlowBeanName() {
        return "combineInputTableWithScore";
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream("combineInputTableWithScore/Result/scored.txt");
        List<String> lines = IOUtils.readLines(is);
        Map<String, Double> scores = new HashMap<>();
        for (String line : lines) {
            String[] arr = line.split(",");
            scores.put(arr[0], Double.valueOf(arr[1]));
        }

        executeDataFlow(getStandardParameters());

        List<GenericRecord> records = readOutput();
        assertEquals(records.size(), scores.size());
        for (GenericRecord record : records) {
            assertNotNull(record.get(InterfaceName.Id.name()));
            assertNotNull(record.get(ScoreResultField.Percentile.displayName));
            assertNotNull(record.get(ScoreResultField.RawScore.name()));
        }
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return InterfaceName.Id.name();
    }
}
