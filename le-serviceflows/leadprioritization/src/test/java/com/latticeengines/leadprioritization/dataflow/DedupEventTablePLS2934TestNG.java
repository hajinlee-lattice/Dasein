package com.latticeengines.leadprioritization.dataflow;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-dataflow-context.xml" })
public class DedupEventTablePLS2934TestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional", enabled = false)
    public void test() {
        DedupEventTableParameters parameters = new DedupEventTableParameters("EventTable");
        executeDataFlow(parameters);
        List<GenericRecord> output = readOutput();

        assertEquals(output.size(), 10321);
    }

    @Override
    protected String getScenarioName() {
        return "pls2934";
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }
}
