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
public class ComputeLiftZeroAvgTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

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
        return "zeroAvg";
    }

    private ComputeLiftParameters prepareInput() {
        ComputeLiftParameters parameters = new ComputeLiftParameters();
        parameters.setInputTableName("ScoreWithInput");
        parameters.setRatingField(InterfaceName.Rating.name());
        parameters.setLiftField(InterfaceName.Lift.name());
        parameters.setModelGuidField(ScoreResultField.ModelId.displayName);
        parameters.setScoreFieldMap(ImmutableMap.of( //
                "ms__d794ef32-3d34-4391-95a9-9d9bd2621dcf-ai_lf__5", "ExpectedRevenue", //
                "ms__1f9435e4-f824-4623-b110-61644dca8a70-ai_1g0mh", "Probability" //
        ));
        return parameters;
    }

    @SuppressWarnings("unused")
    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        int numRows = 0;
        for (GenericRecord record : records) {
            numRows++;
            Assert.assertTrue(record.get(InterfaceName.Lift.name()) instanceof Double);
        }
        Assert.assertEquals(numRows, 2);
    }

}
