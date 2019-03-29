package com.latticeengines.datacloud.workflow.match.steps;

import org.junit.Assert;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.PrepareBulkMatchInputConfiguration;

public class PrepareBulkMatchInputUnitTestNG {

    @Test(groups = "unit", dataProvider = "getMatchInputInfo")
    private void testDetermineBlockSizes(String version, String entity, Boolean fetchOnly, Long count,
            Integer[] expectedBlockSizes) {
        PrepareBulkMatchInput step = mockPrepareBulkMatchInput(version, entity, fetchOnly);
        Integer[] blockSizes = step.determineBlockSizes(count);
        Assert.assertArrayEquals(expectedBlockSizes, blockSizes);
    }

    @DataProvider(name = "getMatchInputInfo")
    private Object[][] provideMatchInputInfo() {
        return new Object[][] {
                // Schema: DataCloudVersion, Entity, IsFetchOnly, TotalCount,
                // ExpectedBlockSizes
                // Case 1: DataCloud based fuzzy match
                // LDC Match
                // count < minBlockSize * maxConcurrentBlocks (1000 * 2)
                { "2.0.17", null, false, 1L, new Integer[] { 1 } }, //
                { "2.0.17", null, false, 1000L, new Integer[] { 1000 } }, //
                { "2.0.17", null, false, 1999L, new Integer[] { 1999 } }, //
                // minBlockSize * maxConcurrentBlocks (1000 * 2) <= count <=
                // maxBlockSize * maxConcurrentBlocks (5000 * 2)
                { "2.0.17", null, false, 2000L, new Integer[] { 1000, 1000 } }, //
                { "2.0.17", null, false, 9999L, new Integer[] { 4999, 5000 } }, //
                { "2.0.17", null, false, 10000L, new Integer[] { 5000, 5000 } }, //
                // count > (maxBlockSize * maxConcurrentBlocks (5000 * 2)
                { "2.0.17", null, false, 10001L, new Integer[] { 5000, 5001 } }, //
                { "2.0.17", null, false, 20000L, new Integer[] { 5000, 5000, 5000, 5000 } }, //

                // LDC Match
                // count < minBlockSize * maxConcurrentBlocks (1000 * 2)
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 1L, new Integer[] { 1 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 1000L, new Integer[] { 1000 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 1999L, new Integer[] { 1999 } }, //
                // minBlockSize * maxConcurrentBlocks (1000 * 2) <= count <=
                // maxBlockSize * maxConcurrentBlocks (5000 * 2)
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 2000L, new Integer[] { 1000, 1000 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 9999L, new Integer[] { 4999, 5000 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 10000L, new Integer[] { 5000, 5000 } }, //
                // count > (maxBlockSize * maxConcurrentBlocks (5000 * 2)
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 10001L, new Integer[] { 5000, 5001 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 20000L,
                        new Integer[] { 5000, 5000, 5000, 5000 } }, //

                // Account Entity Match
                // count < minBlockSize * maxConcurrentBlocks (1000 * 2)
                { "2.0.17", BusinessEntity.Account.name(), false, 1L, new Integer[] { 1 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 1000L, new Integer[] { 1000 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 1999L, new Integer[] { 1999 } }, //
                // minBlockSize * maxConcurrentBlocks (1000 * 2) <= count <=
                // maxBlockSize * maxConcurrentBlocks (5000 * 2)
                { "2.0.17", BusinessEntity.Account.name(), false, 2000L, new Integer[] { 1000, 1000 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 9999L, new Integer[] { 4999, 5000 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 10000L, new Integer[] { 5000, 5000 } }, //
                // count > (maxBlockSize * maxConcurrentBlocks (5000 * 2)
                { "2.0.17", BusinessEntity.Account.name(), false, 10001L, new Integer[] { 5000, 5001 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 20000L, new Integer[] { 5000, 5000, 5000, 5000 } }, //

                // Contact Entity Match
                // count < minBlockSize * maxConcurrentBlocks (1000 * 4)
                { "2.0.17", BusinessEntity.Contact.name(), false, 1L, new Integer[] { 1 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 1000L, new Integer[] { 1000 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 1999L, new Integer[] { 1999 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 2000L, new Integer[] { 1000, 1000 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 3999L, new Integer[] { 1333, 1333, 1333 } }, //
                // minBlockSize * maxConcurrentBlocks (1000 * 4) <= count <=
                // maxBlockSize * maxConcurrentBlocks (5000 * 4)
                { "2.0.17", BusinessEntity.Contact.name(), false, 4000L, new Integer[] { 1000, 1000, 1000, 1000 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 19999L, new Integer[] { 4999, 4999, 4999, 5002 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 20000L, new Integer[] { 5000, 5000, 5000, 5000 } }, //
                // count > (maxBlockSize * maxConcurrentBlocks (5000 * 4)
                { "2.0.17", BusinessEntity.Contact.name(), false, 20001L, new Integer[] { 5000, 5000, 5000, 5001 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 40000L,
                        new Integer[] { 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000 } }, //

                // Transaction Entity Match
                // count < minBlockSize * maxConcurrentBlocks (1000 * 8)
                { "2.0.17", BusinessEntity.Transaction.name(), false, 1L, new Integer[] { 1 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 1000L, new Integer[] { 1000 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 1999L, new Integer[] { 1999 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 2000L, new Integer[] { 1000, 1000 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 3999L, new Integer[] { 1333, 1333, 1333 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 7999L,
                        new Integer[] { 1142, 1142, 1142, 1142, 1142, 1142, 1147 } }, //
                // minBlockSize * maxConcurrentBlocks (1000 * 8) <= count <=
                // maxBlockSize * maxConcurrentBlocks (5000 * 8)
                { "2.0.17", BusinessEntity.Transaction.name(), false, 8000L,
                        new Integer[] { 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 39999L,
                        new Integer[] { 4999, 4999, 4999, 4999, 4999, 4999, 4999, 5006 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 40000L,
                        new Integer[] { 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000 } }, //
                // count > (maxBlockSize * maxConcurrentBlocks (5000 * 8)
                { "2.0.17", BusinessEntity.Transaction.name(), false, 40001L,
                        new Integer[] { 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5001 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 50000L,
                        new Integer[] { 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000 } }, //

                // Case 2: DataCloud based fetch-only match
                // count < minBlockSize * maxConcurrentBlocks (25000 * 4)
                { "2.0.17", null, true, 1L, new Integer[] { 1 } },//
                { "2.0.17", null, true, 25000L, new Integer[] { 25000 } },//
                { "2.0.17", null, true, 99999L, new Integer[] { 33333, 33333, 33333 } }, //
                // minBlockSize * maxConcurrentBlocks (25000 * 4) <= count <=
                // maxBlockSize * maxConcurrentBlocks (120000 * 4)
                { "2.0.17", null, true, 100000L, new Integer[] { 25000, 25000, 25000, 25000 } },//
                { "2.0.17", null, true, 480000L, new Integer[] { 120000, 120000, 120000, 120000 } },//
                // count > (maxBlockSize * maxConcurrentBlocks (120000 * 4)
                { "2.0.17", null, true, 600000L, new Integer[] { 120000, 120000, 120000, 120000, 120000 } },//

                // Case 3: DerivedColumnsCache based SQL lookup match
                // count < averageBlockSize * maxConcurrentBlocks (2500 * 4)
                { "1.0.0", null, false, 1L, new Integer[] { 1 } }, //
                { "1.0.0", null, false, 2499L, new Integer[] { 2499 } },//
                { "1.0.0", null, false, 2500L, new Integer[] { 1250, 1250 } },//
                { "1.0.0", null, false, 5000L, new Integer[] { 1666, 1666, 1668 } },//
                { "1.0.0", null, false, 7500L, new Integer[] { 1875, 1875, 1875, 1875 } },//
                { "1.0.0", null, false, 9999L, new Integer[] { 2499, 2499, 2499, 2502 } },//
        };
    }

    private PrepareBulkMatchInput mockPrepareBulkMatchInput(String version, String entity, boolean fetchOnly) {
        PrepareBulkMatchInput step = new PrepareBulkMatchInput();
        PrepareBulkMatchInputConfiguration config = new PrepareBulkMatchInputConfiguration();
        MatchInput input = new MatchInput();
        input.setDataCloudVersion(version);
        input.setTargetEntity(entity);
        input.setFetchOnly(fetchOnly);
        config.setMatchInput(input);
        config.setAverageBlockSize(2500);
        step.setConfiguration(config);
        ReflectionTestUtils.setField(step, "executionContext", new ExecutionContext());
        ReflectionTestUtils.setField(step, "maxFetchConcurrentBlocks", 4);
        ReflectionTestUtils.setField(step, "maxAccountConcurrentBlocks", 2);
        ReflectionTestUtils.setField(step, "maxContactConcurrentBlocks", 4);
        ReflectionTestUtils.setField(step, "maxTxnConcurrentBlocks", 8);
        ReflectionTestUtils.setField(step, "minFuzzyBlockSize", 1000);
        ReflectionTestUtils.setField(step, "maxFuzzyBlockSize", 5000);
        ReflectionTestUtils.setField(step, "minFetchBlockSize", 25000);
        ReflectionTestUtils.setField(step, "maxFetchBlockSize", 120000);

        return step;
    }
}
