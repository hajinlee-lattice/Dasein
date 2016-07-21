package com.latticeengines.leadprioritization.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-context.xml" })
public class CombineInputTableWithScoreInternalIdTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

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

    @Override
    protected String getScenarioName() {
        return "internalIdBased";
    }

    @Test(groups = "functional")
    public void execute() throws IOException {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters("ScoreResult",
                "InputTable");
        params.enableDebugging();
        executeDataFlow(params);

        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 2);
        for (GenericRecord record : outputRecords) {
            assertNotNull(record.get(InterfaceName.InternalId.name()));
            assertNotNull(record.get(ScoreResultField.Percentile.displayName));
        }
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return InterfaceName.InternalId.name();
    }
}
