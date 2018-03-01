package com.latticeengines.scoring.dataflow;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.ComputeLiftParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class ComputeLiftTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

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
        return "simple";
    }

    private ComputeLiftParameters prepareInput() {
        ComputeLiftParameters parameters = new ComputeLiftParameters();
        parameters.setInputTableName("ScoreWithInput");
        parameters.setRatingField(InterfaceName.Rating.name());
        parameters.setLiftField(InterfaceName.Lift.name());
        parameters.setModelGuidField(ScoreResultField.ModelId.displayName);
        parameters.setScoreFieldMap(ImmutableMap.of( //
                "ms__9c1338b0-7052-4665-8960-a5acf7bfed43-CDLEnd2E", "ExpectedRevenue", //
                "ms__21dd9b0f-f5be-4818-9efd-4ac9be61d41c-CDLEnd2E", "Probability" //
        ));
        return parameters;
    }

    @SuppressWarnings("unused")
    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        int numRows = 0;
        for (GenericRecord record : records) {
            // System.out.println(record);
            numRows++;
        }
        Assert.assertEquals(numRows, 2);
    }

}
