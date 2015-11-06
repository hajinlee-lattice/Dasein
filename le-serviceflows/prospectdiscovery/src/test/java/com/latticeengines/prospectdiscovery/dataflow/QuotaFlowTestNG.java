package com.latticeengines.prospectdiscovery.dataflow;

import com.latticeengines.common.exposed.query.SingleReferenceLookup;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOptionName;
import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsFunctionalTestNGBase;

import java.util.ArrayList;
import java.util.List;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class QuotaFlowTestNG extends ServiceFlowsFunctionalTestNGBase<QuotaFlowParameters> {

    @Test(groups = "functional")
    public void execute() throws Exception {

        Table result = executeDataFlow();

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 3);
    }

    @Override
    protected DataFlowParameters getDataFlowParameters() {
        TargetMarket market = new TargetMarket();
        market.setIntentScoreThreshold(20.0);
        market.setFitScoreThreshold(20.0);
        market.setNumDaysBetweenIntentProspectResends(365);
        market.setModelId("M1");
        market.setNumProspectsDesired(3);
        market.setDeliverProspectsFromExistingAccounts(true);
        market.setIntentSort(new Sort());
        ProspectDiscoveryConfiguration configuration = new ProspectDiscoveryConfiguration();
        configuration.setDouble(ProspectDiscoveryOptionName.IntentPercentage, 100);
        Quota quota = new Quota();
        quota.setBalance(100);
        return new QuotaFlowParameters(market, quota, configuration);
    }

    @Override
    public String getFlowBeanName() {
        return "quotaFlow";
    }
}
