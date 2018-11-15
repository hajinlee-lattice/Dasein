package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = {"classpath:serviceflows-scoring-dataflow-context.xml"})
public class CalculateExpectedRevenuePercentilePLS11356TestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return "calculateExpectedRevenuePercentile";
    }

    @Override
    protected String getScenarioName() {
        return "PLS-11356";
    }

    @Test(groups = "functional")
    public void testCalculationExpectedRevenuePercentile() {
        CalculateExpectedRevenuePercentileParameters parameters = prepareInputWithExpectedRevenue();
        executeDataFlow(parameters);
        verifyResults();
    }

    private void verifyResults() {
        List<GenericRecord> inputRecords = readInput("InputTable");
        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), inputRecords.size());
        outputRecords.forEach(record -> {
            System.out.println(record);
            // Assert.assertNotNull(record.get("Score"));
        });
    }

    @Override
    protected void postProcessSourceTable(Table table) {
        super.postProcessSourceTable(table);
    }

    private CalculateExpectedRevenuePercentileParameters prepareInputWithExpectedRevenue() {
        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("calculateExpectedRevenuePercentile/PLS-11356/params.json");
        CalculateExpectedRevenuePercentileParameters parameters = JsonUtils.deserialize(inputStream, CalculateExpectedRevenuePercentileParameters.class);
        return parameters;
    }

}
