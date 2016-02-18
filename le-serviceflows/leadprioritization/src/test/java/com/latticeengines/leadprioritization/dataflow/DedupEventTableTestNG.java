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
public class DedupEventTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    @Test(groups = "functional")
    public void test() {
        verifySource();

        DedupEventTableParameters parameters = new DedupEventTableParameters("EventTable", "Domain", "Email",
                "Event_IsWon");
        executeDataFlow(parameters);
        List<GenericRecord> output = readOutput();
        Map<Object, Integer> histogram = histogram(output, "Domain");
        Assert.assertTrue(histogram.size() > 0);
        Assert.assertTrue(Iterables.all(histogram.values(), new Predicate<Integer>() {

            @Override
            public boolean apply(Integer input) {
                return input == 1;
            }
        }));
    }

    private void verifySource() {
        List<GenericRecord> input = readInput("EventTable");
        Map<Object, Integer> histogram = histogram(input, "Domain");
        Assert.assertTrue(histogram.size() > 0);
        Assert.assertFalse(Iterables.all(histogram.values(), new Predicate<Integer>() {

            @Override
            public boolean apply(Integer input) {
                return input == 1;
            }
        }));
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return "LastModifiedDate";
    }
}
