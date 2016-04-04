package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatistics;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

@Component
public class RealTimeMatchExecutorTestNG extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    private RealTimeMatchExecutor realTimeMatchExecutor;

    @Test(groups = { "functional" })
    public void testDistributeResults() {
        List<InternalOutputRecord> recordList = realTimeMatchExecutor.distributeResults(prepareRecords(),
                prepareResultsMap());
        Assert.assertNotNull(recordList);
        Assert.assertEquals(recordList.size(), prepareRecords().size());
        verifyDistributedResults(recordList);
    }

    @Test(groups = { "functional" }, dependsOnMethods = "testDistributeResults")
    public void testMergeResults() {
        List<InternalOutputRecord> recordList = realTimeMatchExecutor.distributeResults(prepareRecords(),
                prepareResultsMap());
        MatchContext matchContext = new MatchContext();
        matchContext.setInternalResults(recordList);

        MatchInput input = new MatchInput();
        input.setTenant(new Tenant("PD_Test"));
        input.setPredefinedSelection(ColumnSelection.Predefined.Model);
        matchContext.setInput(input);

        MatchOutput output = new MatchOutput(UUID.randomUUID());
        output.setOutputFields(Arrays.asList("Col1", "Col2", "Col3", "Col4"));
        MatchStatistics statistics = new MatchStatistics();
        output.setStatistics(statistics);
        output.setSubmittedBy(input.getTenant());
        matchContext.setOutput(output);
        matchContext.setColumnPriorityMap(prepareColumnPriorityMap());

        matchContext = realTimeMatchExecutor.mergeResults(matchContext);
        verifyMergedRecords(matchContext.getInternalResults());
    }

    private Map<String, List<Map<String, Object>>> prepareResultsMap() {
        Map<String, List<Map<String, Object>>> map = new HashMap<>();
        for (String source : Arrays.asList("Source1", "Source2")) {
            List<Map<String, Object>> resultList = new ArrayList<>();
            Map<String, Object> row = new HashMap<>();
            row.put("Domain", "a.com");
            row.put("Col1", 1);
            row.put("Col2", "2");
            row.put("Col3", true);
            if (source.equals("Source1")) {
                resultList.add(row);
            }

            row = new HashMap<>();
            row.put("Domain", "b.com");
            row.put("Col1", 2);
            row.put("Col2", "4");
            row.put("Col3", false);
            if (source.equals("Source2")) {
                resultList.add(row);
            }

            row = new HashMap<>();
            row.put("Domain", "c.com");
            row.put("Col1", 3);
            row.put("Col2", "0");
            row.put("Col3", "Source1".equals(source));
            resultList.add(row);

            map.put(source, resultList);
        }
        return map;
    }

    private List<InternalOutputRecord> prepareRecords() {
        List<InternalOutputRecord> recordList = new ArrayList<>();

        InternalOutputRecord record = new InternalOutputRecord();
        record.setParsedDomain("a.com");
        recordList.add(record);

        record = new InternalOutputRecord();
        record.setParsedDomain("b.com");
        recordList.add(record);

        record = new InternalOutputRecord();
        record.setParsedDomain("c.com");
        recordList.add(record);

        record = new InternalOutputRecord();
        record.setParsedDomain("d.com");
        recordList.add(record);

        record = new InternalOutputRecord();
        record.setParsedDomain(null);
        recordList.add(record);

        return recordList;
    }

    private Map<String, List<String>> prepareColumnPriorityMap() {
        Map<String, List<String>> map = new HashMap<>();
        for (String column : Arrays.asList("Col1", "Col2", "Col3", "Col5")) {
            switch (column) {
            case "Col1":
                map.put(column, Collections.singletonList("Source1"));
                break;
            case "Col2":
                map.put(column, Collections.singletonList("Source2"));
                break;
            case "Col3":
                map.put(column, Arrays.asList("Source1", "Source2"));
                break;
            default:
                map.put(column, new ArrayList<String>());
            }
        }
        return map;
    }

    private void verifyDistributedResults(List<InternalOutputRecord> records) {
        for (InternalOutputRecord record : records) {
            Map<String, Map<String, Object>> results = record.getResultsInSource();
            if (record.getParsedDomain() != null) {
                switch (record.getParsedDomain()) {
                case "a.com":
                    Assert.assertTrue(results.containsKey("Source1"),
                            record.getParsedDomain() + " should match Source1");
                    Assert.assertFalse(results.containsKey("Source2"),
                            record.getParsedDomain() + " should match Source2");
                    break;
                case "b.com":
                    Assert.assertFalse(results.containsKey("Source1"),
                            record.getParsedDomain() + " should not match Source1");
                    Assert.assertTrue(results.containsKey("Source2"),
                            record.getParsedDomain() + " should match Source2");
                    break;
                case "c.com":
                    Assert.assertTrue(results.containsKey("Source1"),
                            record.getParsedDomain() + " should match Source1");
                    Assert.assertTrue(results.containsKey("Source2"),
                            record.getParsedDomain() + " should match Source2");
                    break;
                default:
                    Assert.assertFalse(results.containsKey("Source1"),
                            record.getParsedDomain() + " should not match Source1");
                    Assert.assertFalse(results.containsKey("Source2"),
                            record.getParsedDomain() + " should not match Source2");

                }
            } else {
                Assert.assertFalse(results.containsKey("Source1"), "null domain should not match Source1");
                Assert.assertFalse(results.containsKey("Source2"), "null domain should not match Source2");
            }
        }
    }

    private void verifyMergedRecords(List<InternalOutputRecord> records) {
        for (InternalOutputRecord record : records) {
            List<Object> output = record.getOutput();

            Assert.assertEquals(output.size(), 4);
            Assert.assertEquals(output.get(3), null);

            if (record.getParsedDomain() != null) {
                switch (record.getParsedDomain()) {
                case "a.com":
                    Assert.assertTrue(record.isMatched(), record.getParsedDomain() + " should be matched.");
                    Assert.assertEquals(output.get(0), 1);
                    Assert.assertEquals(output.get(1), null);
                    Assert.assertEquals(output.get(2), true);
                    break;
                case "b.com":
                    Assert.assertTrue(record.isMatched(), record.getParsedDomain() + " should be matched.");
                    Assert.assertEquals(output.get(0), null);
                    Assert.assertEquals(output.get(1), "4");
                    Assert.assertEquals(output.get(2), false);
                    break;
                case "c.com":
                    Assert.assertTrue(record.isMatched(), record.getParsedDomain() + " should be matched.");
                    Assert.assertEquals(output.get(0), 3);
                    Assert.assertEquals(output.get(1), "0");
                    Assert.assertEquals(output.get(2), false);
                    break;
                default:
                    Assert.assertEquals(output.get(0), null);
                    Assert.assertEquals(output.get(1), null);
                    Assert.assertEquals(output.get(2), null);
                }
            } else {
                Assert.assertEquals(output.get(0), null);
                Assert.assertEquals(output.get(1), null);
                Assert.assertEquals(output.get(2), null);
            }
        }
    }

}
