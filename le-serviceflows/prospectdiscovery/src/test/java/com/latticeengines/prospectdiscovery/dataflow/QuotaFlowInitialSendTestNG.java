package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
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
public class QuotaFlowInitialSendTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private QuotaFlowParameters getStandardParameters() {
        TargetMarket market = new TargetMarket();
        TargetMarketDataFlowProperty marketConfiguration = market.getDataFlowConfiguration();
        marketConfiguration.setString(TargetMarketDataFlowOptionName.IntentScoreThreshold, IntentScore.LOW.toString());
        marketConfiguration.setDouble(TargetMarketDataFlowOptionName.FitScoreThreshold, 0.0);
        marketConfiguration.set(TargetMarketDataFlowOptionName.NumDaysBetweenIntentProspecResends, 365);
        marketConfiguration.setBoolean(TargetMarketDataFlowOptionName.DeliverProspectsFromExistingAccounts, true);

        market.setModelId("M1");
        market.setNumProspectsDesired(100);
        List<String> intent = new ArrayList<>();
        intent.add("Intent1");
        intent.add("Intent2");
        market.setSelectedIntent(intent);
        market.setOffset(1);
        ProspectDiscoveryProperty configuration = new ProspectDiscoveryProperty();
        configuration.setDouble(ProspectDiscoveryOptionName.IntentPercentage, 50);
        Quota quota = new Quota();
        quota.setBalance(100);
        return new QuotaFlowParameters(market, quota, configuration);
    }

    @Test(groups = "functional")
    public void testAllProspectsSent() {
        setEngine("TEZ");
        Table result = executeDataFlow(getStandardParameters());

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();

        List<GenericRecord> scores = readInput("Scores");
        Assert.assertTrue(identicalSets(records, QuotaFlow.LatticeAccountID, scores, QuotaFlow.LatticeAccountID));
    }

    @Test(groups = "functional")
    public void testOnlyFitSent() {
        setEngine("TEZ");
        QuotaFlowParameters parameters = getStandardParameters();
        TargetMarketDataFlowProperty dataFlowConfiguration = parameters.getTargetMarket().getDataFlowConfiguration();
        dataFlowConfiguration.set(TargetMarketDataFlowOptionName.IntentScoreThreshold, IntentScore.MAX.toString());

        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();

        List<GenericRecord> scores = readInput("Scores");
        Assert.assertTrue(identicalSets(records, QuotaFlow.LatticeAccountID, scores, QuotaFlow.LatticeAccountID));
        Assert.assertTrue(allValuesEqual(records, "IsIntent", false));
    }

    @Test(groups = "functional")
    public void testOnlyIntentSent() {
        setEngine("TEZ");
        QuotaFlowParameters parameters = getStandardParameters();
        TargetMarketDataFlowProperty dataFlowConfiguration = parameters.getTargetMarket().getDataFlowConfiguration();
        dataFlowConfiguration.setDouble(TargetMarketDataFlowOptionName.FitScoreThreshold, 100.0);

        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();

        Assert.assertTrue(allValuesEqual(records, "IsIntent", true));
    }

    @Test(groups = "functional")
    public void testNoProspectsDesired() {
        setEngine("TEZ");
        QuotaFlowParameters parameters = getStandardParameters();
        parameters.getTargetMarket().setNumProspectsDesired(0);

        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 0);
    }

    @Test(groups = "functional")
    public void testNoMoreThanOneProspectPerAccount() {
        setEngine("TEZ");
        QuotaFlowParameters parameters = getStandardParameters();
        parameters.getTargetMarket().getDataFlowConfiguration()
                .setInt(TargetMarketDataFlowOptionName.MaxProspectsPerAccount, 1);

        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();

        Map<Object, Integer> histogram = histogram(records, QuotaFlow.LatticeAccountID);

        Assert.assertTrue(histogram.size() > 0);
        Assert.assertTrue(Iterables.all(histogram.values(), new Predicate<Integer>() {

            @Override
            public boolean apply(Integer input) {
                return input == 1;
            }
        }));
    }

    @Test(groups = "functional")
    public void testLimitByBalance() {
        setEngine("TEZ");
        QuotaFlowParameters parameters = getStandardParameters();
        parameters.getQuota().setBalance(2);

        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 2);
    }

    @Override
    public String getFlowBeanName() {
        return "quotaFlow";
    }

    @Override
    public String getScenarioName() {
        return "initialSend";
    }
}
