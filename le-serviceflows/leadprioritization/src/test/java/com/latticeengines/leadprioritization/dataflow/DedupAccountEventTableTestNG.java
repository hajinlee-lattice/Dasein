package com.latticeengines.leadprioritization.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-context.xml" })
public class DedupAccountEventTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    @Test(groups = "functional")
    public void test() {
        verifySource();

        DedupEventTableParameters parameters = new DedupEventTableParameters("EventTable");
        executeDataFlow(parameters);
        List<GenericRecord> output = readOutput();
        final Map<Object, Integer> histogram = histogram(output, "Domain");
        Assert.assertTrue(histogram.size() > 0);
        Assert.assertTrue(Iterables.all(histogram.keySet(), new Predicate<Object>() {

            @Override
            public boolean apply(Object domain) {
                int qty = histogram.get(domain);
                return qty == 1 || domain == null || domain.toString().equals("");
            }
        }));
    }

    private void verifySource() {
        List<GenericRecord> input = readInput("EventTable");
        final Map<Object, Integer> histogram = histogram(input, "Domain");
        Assert.assertTrue(histogram.size() > 0);
        Assert.assertFalse(Iterables.all(histogram.keySet(), new Predicate<Object>() {

            @Override
            public boolean apply(Object domain) {
                int qty = histogram.get(domain);
                return qty == 1 || domain == null || domain.toString().equals("");
            }
        }));
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }

    @Override
    protected String getScenarioName() {
        return "accountBased";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return "LastModifiedDate";
    }
}
