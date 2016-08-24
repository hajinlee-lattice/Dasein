package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.Column;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatistics;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

//@Component
public class RealTimeMatchExecutorTestNG extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    @Qualifier("realTimeMatchExecutor")
    private RealTimeMatchExecutor realTimeMatchExecutor;

    private static final String COLUMN1 = "BusinessIndustry";
    private static final String COLUMN2 = "AlexaOnlineSince";
    private static final String COLUMN3 = "AlexaRelatedLinks";
    private static final String COLUMN4 = "TechIndicator_Splunk";

    private static final String PARTITION1 = "DerivedColumnsCache";
    private static final String PARTITION2 = "DerivedColumnsCache_TechIndicator";

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
        input.setCustomSelection(prepareColumnSelection());
        matchContext.setInput(input);

        matchContext.setColumnSelection(prepareColumnSelection());

        MatchOutput output = new MatchOutput(UUID.randomUUID());
        output.setOutputFields(Arrays.asList(COLUMN1, COLUMN2, COLUMN3, COLUMN4));
        MatchStatistics statistics = new MatchStatistics();
        output.setStatistics(statistics);
        output.setSubmittedBy(input.getTenant());
        matchContext.setOutput(output);

        matchContext = realTimeMatchExecutor.mergeResults(matchContext);
        verifyMergedRecords(matchContext.getInternalResults());
    }

    private Map<String, List<Map<String, Object>>> prepareResultsMap() {
        Map<String, List<Map<String, Object>>> map = new HashMap<>();

        List<Map<String, Object>> resultList = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("Domain", "a.com");
        row.put(COLUMN1, 1);
        row.put(COLUMN2, "2");
        row.put(COLUMN3, true);
        resultList.add(row);

        row = new HashMap<>();
        row.put("Domain", "c.com");
        row.put(COLUMN1, 3);
        row.put(COLUMN2, "0");
        row.put(COLUMN3, false);
        resultList.add(row);
        map.put(PARTITION1, resultList);

        resultList = new ArrayList<>();
        row = new HashMap<>();
        row.put("Domain", "a.com");
        row.put(COLUMN4, "YES");
        resultList.add(row);

        row = new HashMap<>();
        row.put("Domain", "b.com");
        row.put(COLUMN4, "NO");
        resultList.add(row);

        map.put(PARTITION2, resultList);
        return map;
    }

    private ColumnSelection prepareColumnSelection() {
        ColumnSelection columnSelection = new ColumnSelection();

        List<Column> columns = new ArrayList<>();

        for (String col : Arrays.asList(COLUMN1, COLUMN2, COLUMN3, COLUMN4)) {
            Column column = new Column();
            column.setExternalColumnId(col);
            column.setColumnName(col);
            columns.add(column);
        }

        columnSelection.setColumns(columns);

        return columnSelection;
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

    private void verifyDistributedResults(List<InternalOutputRecord> records) {
        for (InternalOutputRecord record : records) {
            Map<String, Map<String, Object>> results = record.getResultsInPartition();
            if (record.getParsedDomain() != null) {
                switch (record.getParsedDomain()) {
                case "a.com":
                    Assert.assertTrue(results.containsKey(PARTITION1),
                            record.getParsedDomain() + " should match " + PARTITION1);
                    Assert.assertTrue(results.containsKey(PARTITION2),
                            record.getParsedDomain() + " should match " + PARTITION2);
                    break;
                case "b.com":
                    Assert.assertFalse(results.containsKey(PARTITION1),
                            record.getParsedDomain() + " should not match " + PARTITION1);
                    Assert.assertTrue(results.containsKey(PARTITION2),
                            record.getParsedDomain() + " should match " + PARTITION2);
                    break;
                case "c.com":
                    Assert.assertTrue(results.containsKey(PARTITION1),
                            record.getParsedDomain() + " should match " + PARTITION1);
                    Assert.assertFalse(results.containsKey(PARTITION2),
                            record.getParsedDomain() + " should not match " + PARTITION2);
                    break;
                default:
                    Assert.assertFalse(results.containsKey(PARTITION1),
                            record.getParsedDomain() + " should not match " + PARTITION1);
                    Assert.assertFalse(results.containsKey(PARTITION2),
                            record.getParsedDomain() + " should not match " + PARTITION2);

                }
            } else {
                Assert.assertFalse(results.containsKey(PARTITION1), "null should not match " + PARTITION1);
                Assert.assertFalse(results.containsKey(PARTITION2), "null should not match " + PARTITION2);
            }
        }
    }

    private void verifyMergedRecords(List<InternalOutputRecord> records) {
        for (InternalOutputRecord record : records) {
            List<Object> output = record.getOutput();

            Assert.assertEquals(output.size(), 4);

            if (record.getParsedDomain() != null) {
                switch (record.getParsedDomain()) {
                case "a.com":
                    Assert.assertTrue(record.isMatched(), record.getParsedDomain() + " should be matched.");
                    Assert.assertEquals(output.get(0), 1);
                    Assert.assertEquals(output.get(1), "2");
                    Assert.assertEquals(output.get(2), true);
                    Assert.assertEquals(output.get(3), "YES");
                    break;
                case "b.com":
                    Assert.assertTrue(record.isMatched(), record.getParsedDomain() + " should be matched.");
                    Assert.assertEquals(output.get(0), null);
                    Assert.assertEquals(output.get(1), null);
                    Assert.assertEquals(output.get(2), null);
                    Assert.assertEquals(output.get(3), "NO");
                    break;
                case "c.com":
                    Assert.assertTrue(record.isMatched(), record.getParsedDomain() + " should be matched.");
                    Assert.assertEquals(output.get(0), 3);
                    Assert.assertEquals(output.get(1), "0");
                    Assert.assertEquals(output.get(2), false);
                    Assert.assertEquals(output.get(3), null);
                    break;
                default:
                    Assert.assertEquals(output.get(0), null);
                    Assert.assertEquals(output.get(1), null);
                    Assert.assertEquals(output.get(2), null);
                    Assert.assertEquals(output.get(3), null);
                }
            } else {
                Assert.assertEquals(output.get(0), null);
                Assert.assertEquals(output.get(1), null);
                Assert.assertEquals(output.get(2), null);
                Assert.assertEquals(output.get(3), null);
            }
        }
    }

}
