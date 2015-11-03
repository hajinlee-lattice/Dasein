package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

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

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class QuotaFlowTestNG extends ServiceFlowsFunctionalTestNGBase<QuotaFlowParameters> {

    @Test(groups = "functional", enabled = true)
    public void execute() throws Exception {

        Table result = executeDataFlow();

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);
    }

    @Override
    protected DataFlowParameters getDataFlowParameters() {
        List<TargetMarket> markets = new ArrayList<>();
        TargetMarket market = new TargetMarket();
        market.setIntentScoreThreshold(20.0);
        market.setFitScoreThreshold(20.0);
        market.setNumDaysBetweenIntentProspectResends(1);
        markets.add(market);

        ProspectDiscoveryConfiguration configuration = new ProspectDiscoveryConfiguration();
        Quota quota = new Quota();
        quota.setBalance(100);
        return new QuotaFlowParameters(markets, quota, configuration);
    }

    @Override
    public String getFlowBeanName() {
        return "quotaFlow";
    }
}
