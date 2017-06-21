package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.dataflow.QuotaFlowParameters;
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
public class QuotaFlowSomeProspectsAlreadySentTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    protected QuotaFlowParameters getStandardParameters() {
        setEngine("TEZ");
        TargetMarket market = new TargetMarket();
        TargetMarketDataFlowProperty marketConfiguration = market.getDataFlowConfiguration();
        marketConfiguration.setString(TargetMarketDataFlowOptionName.IntentScoreThreshold, IntentScore.LOW.toString());
        marketConfiguration.setDouble(TargetMarketDataFlowOptionName.FitScoreThreshold, 20.0);
        marketConfiguration.set(TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends, 365);
        marketConfiguration.setBoolean(TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts, true);

        market.setModelId("M1");
        market.setNumProspectsDesired(3);
        List<String> intent = new ArrayList<>();
        intent.add("Intent1");
        intent.add("Intent2");
        market.setSelectedIntent(intent);
        market.setOffset(1);
        ProspectDiscoveryProperty configuration = new ProspectDiscoveryProperty();
        configuration.setDouble(ProspectDiscoveryOptionName.IntentPercentage, 100);
        Quota quota = new Quota();
        quota.setBalance(100);
        return new QuotaFlowParameters(market, quota, configuration);
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        setEngine("TEZ");

        Table result = executeDataFlow(getStandardParameters());

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 3);
    }

    @Test(groups = "functional")
    public void testOnlyAsongSent() {
        setEngine("TEZ");
        QuotaFlowParameters parameters = getStandardParameters();
        // This should filter out everything so that only the single contact
        // that was never sent out is sent.
        TargetMarketDataFlowProperty dataFlowConfiguration = parameters.getTargetMarket().getDataFlowConfiguration();
        dataFlowConfiguration.set(TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends, null);
        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 1);

        Assert.assertTrue(containsValues(records, "Email", "asong@uog.com"));
        Assert.assertTrue(allValuesEqual(records, "IsIntent", true));
    }

    @Test(groups = "functional")
    public void testFilterExistingAccounts() {
        setEngine("TEZ");
        QuotaFlowParameters parameters = getStandardParameters();
        TargetMarketDataFlowProperty dataFlowConfiguration = parameters.getTargetMarket().getDataFlowConfiguration();
        dataFlowConfiguration.setBoolean( //
                TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts, false);
        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 0);
    }

    @Override
    public String getFlowBeanName() {
        return "quotaFlow";
    }

    @Override
    public String getScenarioName() {
        return "someProspectsAlreadySent";
    }
}
