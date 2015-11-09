package com.latticeengines.prospectdiscovery.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.query.ReferenceInterpretation;
import com.latticeengines.common.exposed.query.SingleReferenceLookup;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOptionName;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class QuotaFlowInitialSendTestNG extends ServiceFlowsFunctionalTestNGBase {

    private QuotaFlowParameters getStandardParameters() {
        TargetMarket market = new TargetMarket();
        market.setIntentScoreThreshold(0.0);
        market.setFitScoreThreshold(0.0);
        market.setNumDaysBetweenIntentProspectResends(365);
        market.setModelId("M1");
        market.setNumProspectsDesired(100);
        market.setDeliverProspectsFromExistingAccounts(true);
        List<SingleReferenceLookup> lookups = new ArrayList<>();
        lookups.add(new SingleReferenceLookup("Intent1", ReferenceInterpretation.COLUMN));
        lookups.add(new SingleReferenceLookup("Intent2", ReferenceInterpretation.COLUMN));
        market.setIntentSort(new Sort(lookups, true));
        market.setOffset(1);
        ProspectDiscoveryConfiguration configuration = new ProspectDiscoveryConfiguration();
        configuration.setDouble(ProspectDiscoveryOptionName.IntentPercentage, 50);
        Quota quota = new Quota();
        quota.setBalance(100);
        return new QuotaFlowParameters(market, quota, configuration);
    }

    @Test(groups = "functional")
    public void testAllProspectsSent() {
        Table result = executeDataFlow(getStandardParameters());

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();

        List<GenericRecord> scores = readInput("Scores");
        Assert.assertTrue(identicalSets(records, "Id", scores, "Id"));
    }

    @Test(groups = "functional")
    public void testOnlyFitSent() {
        QuotaFlowParameters parameters = getStandardParameters();
        parameters.getTargetMarket().setIntentScoreThreshold(100.0);

        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();

        List<GenericRecord> scores = readInput("Scores");
        Assert.assertTrue(identicalSets(records, "Id", scores, "Id"));
        Assert.assertTrue(allValuesEqual(records, "IsIntent", false));
    }

    @Test(groups = "functional")
    public void testOnlyIntentSent() {
        QuotaFlowParameters parameters = getStandardParameters();
        parameters.getTargetMarket().setFitScoreThreshold(100.0);

        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();

        Assert.assertTrue(allValuesEqual(records, "IsIntent", true));
    }

    @Test(groups = "functional")
    public void testNoProspectsDesired() {
        QuotaFlowParameters parameters = getStandardParameters();
        parameters.getTargetMarket().setNumProspectsDesired(0);

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
        return "initialSend";
    }
}
