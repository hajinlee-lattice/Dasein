package com.latticeengines.leadprioritization.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

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
            if (scores.containsKey(record.get(InterfaceName.Id.name()).toString())) {
                assertNotNull(record.get("Percentile"));
                assertNull(record.get("LeadID"));
                assertNull(record.get("LeadID_Str"));
                assertNull(record.get("Play_Display_Name"));
                assertNull(record.get("Probability"));
                assertNull(record.get("Score"));
                assertNull(record.get("Lift"));
                assertNull(record.get("Bucket_Display_Name"));
                assertTrue(Math.abs(scores.get(record.get(InterfaceName.Id.name()).toString())
                        - ((Double) (record.get("RawScore")))) < 0.000001);
            }
        }

    }

    @Override
    protected String getIdColumnName(String tableName) {
        if (tableName.equals("ScoreResult")) {
            return "LeadID";
        } else if (tableName.equals("InputTable")) {
            return InterfaceName.Id.name();
        }
        return InterfaceName.Id.name();
    }
}
