package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class CalculateExpectedRevenuePercentilePLS11356TestNG extends ScoringServiceFlowsDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return "calculateExpectedRevenuePercentile";
    }

    @Override
    protected String getScenarioName() {
        return "PLS-11356";
    }

    @Override
    protected String getExecutionEngine() {
        return "TEZ";
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
        AtomicInteger evRecordCount = new AtomicInteger(0);
        AtomicInteger nonEvRecordCount = new AtomicInteger(0);
        outputRecords.forEach(record -> {
            Assert.assertNotNull(record.get(ScoreResultField.Percentile.displayName));
            if (record.get(ScoreResultField.ExpectedRevenuePercentile.displayName) != null) {
                Assert.assertEquals(record.get(ScoreResultField.Percentile.displayName),
                        record.get(ScoreResultField.ExpectedRevenuePercentile.displayName));
                Assert.assertNotNull(record.get(ScoreResultField.ExpectedRevenue.displayName));
                evRecordCount.getAndIncrement();
            } else {
                Assert.assertNull(record.get(ScoreResultField.ExpectedRevenue.displayName));
                nonEvRecordCount.getAndIncrement();
            }
        });
        Assert.assertEquals(evRecordCount.get(), 19179);
        Assert.assertEquals(nonEvRecordCount.get(), 103457);
    }

    @Override
    protected void postProcessSourceTable(Table table) {
        super.postProcessSourceTable(table);
    }

    private CalculateExpectedRevenuePercentileParameters prepareInputWithExpectedRevenue() {
        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("calculateExpectedRevenuePercentile/PLS-11356/params.json");
        CalculateExpectedRevenuePercentileParameters parameters = JsonUtils.deserialize(inputStream,
                CalculateExpectedRevenuePercentileParameters.class);

        setFitFunctionParametersMap(parameters);
        return parameters;
    }

}
