package com.latticeengines.prospectdiscovery.dataflow;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class QuotaFlowTestNG extends ServiceFlowsFunctionalTestNGBase<QuotaFlowParameters> {

    @Test(groups = "functional", enabled = false)
    public void execute() throws Exception {
        Table result = executeDataFlow();

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> outputTable = readTable(result.getExtracts().get(0).getPath() + "/*.avro");
    }

    @Override
    public String getFlowBeanName() {
        return "quotaFlow";
    }
}
