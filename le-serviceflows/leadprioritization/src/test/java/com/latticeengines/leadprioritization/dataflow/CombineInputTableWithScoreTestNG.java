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
        List<GenericRecord> inputRecords = AvroUtils.readFromLocalFile(ClassLoader.getSystemResource(
                String.format("%s/%s/part-m-00000.avro", //
                        getFlowBeanName(),getStandardParameters().getInputTableName())) //
                        .getPath());

        executeDataFlow(getStandardParameters());

        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), inputRecords.size());
        for (GenericRecord record : outputRecords) {
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
