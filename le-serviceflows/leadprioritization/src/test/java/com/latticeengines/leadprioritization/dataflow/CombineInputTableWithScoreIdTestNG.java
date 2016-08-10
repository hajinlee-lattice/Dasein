package com.latticeengines.leadprioritization.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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
public class CombineInputTableWithScoreIdTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private CombineInputTableWithScoreParameters getStandardParameters() {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters("ScoreResult",
                "InputTable");
        return params;
    }

    @Override
    protected String getFlowBeanName() {
        return "combineInputTableWithScore";
    }

    @Override
    protected String getScenarioName() {
        return "idBased";
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        List<GenericRecord> inputRecords = AvroUtils.readFromLocalFile(ClassLoader.getSystemResource(
                String.format("%s/%s/%s/part-m-00000.avro", //
                        getFlowBeanName(), getScenarioName(), getStandardParameters().getInputTableName())) //
                .getPath());

        executeDataFlow(getStandardParameters());

        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), inputRecords.size());
        for (GenericRecord record : outputRecords) {
            assertNotNull(record.get(InterfaceName.Id.name()));
            assertNotNull(record.get(ScoreResultField.Percentile.displayName));
        }
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return InterfaceName.Id.name();
    }
}
