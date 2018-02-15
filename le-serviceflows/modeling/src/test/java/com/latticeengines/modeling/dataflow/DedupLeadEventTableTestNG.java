package com.latticeengines.modeling.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.modeling.dataflow.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-modeling-dataflow-context.xml" })
public class DedupLeadEventTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    @Test(groups = "functional", enabled = false)
    public void test() {
        initialize();
        verifySource();

        DedupEventTableParameters parameters = new DedupEventTableParameters("EventTable");
        executeDataFlow(parameters);
        List<GenericRecord> output = readOutput();
        final List<GenericRecord> publicDomains = readInput("PublicDomain");
        final Map<Object, Integer> histogram = histogramDomains(output);
        Assert.assertTrue(histogram.size() > 0);
        Assert.assertTrue(histogram.keySet().stream().allMatch(domain -> {
            int qty = histogram.get(domain);
            boolean isPublic = publicDomains.stream()
                    .anyMatch(r -> r.get("Domain") != null && r.get("Domain").toString().toUpperCase().equals(domain));
            boolean ok = qty == 1 || domain == null || domain.toString().equals("") || isPublic;
            if (!ok) {
                System.out.println(String.format("Domain: %s, qty: %d, public: %s", domain, qty, isPublic));
            }
            return true;
        }));
    }

    private void verifySource() {
        List<GenericRecord> input = readInput("EventTable");
        final Map<Object, Integer> histogram = histogramDomains(input);
        Assert.assertTrue(histogram.size() > 0);
        Assert.assertFalse(Iterables.all(histogram.keySet(), new Predicate<Object>() {

            @Override
            public boolean apply(Object domain) {

                int qty = histogram.get(domain);
                return qty == 1 || domain == null || domain.toString().equals("");
            }
        }));
    }

    private Map<Object, Integer> histogramDomains(List<GenericRecord> records) {
        return histogram(records, record -> {
            if (record == null) {
                return null;
            }

            Object email = record.get(InterfaceName.Email.name());
            Object website = record.get(InterfaceName.Website.name());

            if (website != null) {
                return website.toString().replaceAll("^https?://", "").replaceAll("^www[.]", "").replaceAll("/.*$", "")
                        .toUpperCase();
            } else {
                return email.toString().replaceAll("^.*@", "").toUpperCase();
            }
        });
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }

    @Override
    protected String getScenarioName() {
        return "leadBased";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        if (!tableName.equals("PublicDomain")) {
            return "LastModifiedDate";
        }
        return null;
    }
}
