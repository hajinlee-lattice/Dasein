package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.IntentScore;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOptionName;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryProperty;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowOptionName;
import com.latticeengines.domain.exposed.pls.TargetMarketDataFlowProperty;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-dataflow-context.xml" })
public class QuotaFlowLargeDataTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private QuotaFlowParameters getStandardParameters() {
        setEngine("TEZ");
        TargetMarket market = new TargetMarket();
        TargetMarketDataFlowProperty marketConfiguration = market.getDataFlowConfiguration();
        marketConfiguration.setString(TargetMarketDataFlowOptionName.IntentScoreThreshold, IntentScore.LOW.toString());
        marketConfiguration.setDouble(TargetMarketDataFlowOptionName.FitScoreThreshold, 0.0);
        marketConfiguration.set(TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends, null);
        marketConfiguration.setBoolean(TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts, true);

        market.setModelId("M1");
        market.setNumProspectsDesired(1000000);
        List<String> intent = new ArrayList<>();
        intent.add("Intent1");
        intent.add("Intent2");
        market.setSelectedIntent(intent);
        market.setOffset(1);
        ProspectDiscoveryProperty configuration = new ProspectDiscoveryProperty();
        configuration.setDouble(ProspectDiscoveryOptionName.IntentPercentage, 50);
        Quota quota = new Quota();
        quota.setBalance(1000000);
        return new QuotaFlowParameters(market, quota, configuration);
    }

    @Test(groups = "functional", enabled = true)
    public void test() {
        setEngine("TEZ");
        Table result = executeDataFlow(getStandardParameters());

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> accounts = readInput("AccountMaster");
        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), accounts.size());
    }

    @Override
    public String getFlowBeanName() {
        return "quotaFlow";
    }

    @Override
    public String getScenarioName() {
        return "largeData";
    }
}
