package com.latticeengines.scoring.dataflow;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.ComputeLiftParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class ComputeLiftForEventTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        ComputeLiftParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    @Override
    protected String getFlowBeanName() {
        return "computeLift";
    }

    @Override
    protected String getScenarioName() {
        return "eventTable";
    }

    private ComputeLiftParameters prepareInput() {
        ComputeLiftParameters parameters = new ComputeLiftParameters();
        parameters.setInputTableName("ScoreWithInput");
        parameters.setRatingField(InterfaceName.Rating.name());
        parameters.setLiftField(InterfaceName.Lift.name());
        parameters.setModelGuidField(ScoreResultField.ModelId.displayName);
        parameters.setScoreFieldMap(ImmutableMap.of( //
                "ms__ba5e4d8a-eead-4995-bf9f-efdc514aa66f-SelfServ", "Event" //
        ));
        return parameters;
    }

    @SuppressWarnings("unused")
    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        int numRows = 0;
        for (GenericRecord record : records) {
            System.out.println(record);
            numRows++;
        }
    }

}
