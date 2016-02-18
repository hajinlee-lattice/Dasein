package com.latticeengines.propdata.dataflow.common;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.dataflow.CountFlowParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-propdata-context.xml" })
public class CountFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        CountFlowParameters parameters = new CountFlowParameters("Source");

        executeDataFlow(parameters);
        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
        }
        Long count = (Long) output.get(0).get(CountFlow.COUNT);
        Assert.assertEquals(count, (Long) 1269L);
        Assert.assertEquals(output.size(), 1);
    }

    @Override
    public String getFlowBeanName() {
        return "countFlow";
    }

}
