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
import com.latticeengines.domain.exposed.pls.IntentScore;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOptionName;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class QuotaFlowSomeProspectsAlreadySentTestNG extends ServiceFlowsFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() throws Exception {

        Table result = executeDataFlow(getStandardParameters());

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 3);
    }

    @Test(groups = "functional")
    public void testOnlyAsongSent() {
        QuotaFlowParameters parameters = getStandardParameters();
        // This should filter out everything so that only the single contact
        // that was never sent out is sent.
        parameters.getTargetMarket().setNumDaysBetweenIntentProspectResends(null);
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
        QuotaFlowParameters parameters = getStandardParameters();
        parameters.getTargetMarket().setDeliverProspectsFromExistingAccounts(false);
        Table result = executeDataFlow(parameters);

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 0);
    }

    protected QuotaFlowParameters getStandardParameters() {
        TargetMarket market = new TargetMarket();
        market.setIntentScoreThreshold(IntentScore.LOW);
        market.setFitScoreThreshold(20.0);
        market.setNumDaysBetweenIntentProspectResends(365);
        market.setModelId("M1");
        market.setNumProspectsDesired(3);
        market.setDeliverProspectsFromExistingAccounts(true);
        List<SingleReferenceLookup> lookups = new ArrayList<>();
        lookups.add(new SingleReferenceLookup("Intent1", ReferenceInterpretation.COLUMN));
        lookups.add(new SingleReferenceLookup("Intent2", ReferenceInterpretation.COLUMN));
        market.setIntentSort(new Sort(lookups, true));
        market.setOffset(1);
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

    @Override
    public String getScenarioName() {
        return "someProspectsAlreadySent";
    }
}
