package com.latticeengines.datacloud.workflow.match.steps;

import org.junit.Assert;
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

    // TODO: Only covered newly added functionality of multi-block for fuzzy
    // match. For existing scenario (1.0 matcher, fetch-only), test to be added
    // later
    @DataProvider(name = "getMatchInputInfo")
    private Object[][] provideMatchInputInfo() {
        // Schema: DataCloudVersion, Entity, IsFetchOnly, TotalCount,
        // ExpectedBlockSizes
        return new Object[][] {
                { "2.0.17", null, false, 50L, new Integer[] { 50 } }, //
                { "2.0.17", null, false, 75L, new Integer[] { 75 } }, //
                { "2.0.17", null, false, 100L, new Integer[] { 50, 50 } }, //
                { "2.0.17", null, false, 200L, new Integer[] { 100, 100 } }, //
                { "2.0.17", null, false, 1000L, new Integer[] { 500, 500 } }, //

                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 50L, new Integer[] { 50 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 75L, new Integer[] { 75 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 100L, new Integer[] { 50, 50 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 200L, new Integer[] { 100, 100 } }, //
                { "2.0.17", BusinessEntity.LatticeAccount.name(), false, 1000L, new Integer[] { 500, 500 } }, //

                { "2.0.17", BusinessEntity.Account.name(), false, 50L, new Integer[] { 50 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 75L, new Integer[] { 75 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 100L, new Integer[] { 50, 50 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 200L, new Integer[] { 100, 100 } }, //
                { "2.0.17", BusinessEntity.Account.name(), false, 1000L, new Integer[] { 500, 500 } }, //

                { "2.0.17", BusinessEntity.Contact.name(), false, 50L, new Integer[] { 50 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 100L, new Integer[] { 50, 50 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 200L, new Integer[] { 66, 66, 68 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 400L, new Integer[] { 100, 100, 100, 100 } }, //
                { "2.0.17", BusinessEntity.Contact.name(), false, 1000L, new Integer[] { 250, 250, 250, 250 } }, //

                { "2.0.17", BusinessEntity.Transaction.name(), false, 50L, new Integer[] { 50 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 100L, new Integer[] { 50, 50 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 200L, new Integer[] { 66, 66, 68 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 400L, new Integer[] { 80, 80, 80, 80, 80 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 800L,
                        new Integer[] { 100, 100, 100, 100, 100, 100, 100, 100 } }, //
                { "2.0.17", BusinessEntity.Transaction.name(), false, 1000L,
                        new Integer[] { 125, 125, 125, 125, 125, 125, 125, 125 } }, //
        };
    };

    private PrepareBulkMatchInput mockPrepareBulkMatchInput(String version, String entity, boolean fetchOnly) {
        PrepareBulkMatchInput step = new PrepareBulkMatchInput();
        PrepareBulkMatchInputConfiguration config = new PrepareBulkMatchInputConfiguration();
        MatchInput input = new MatchInput();
        input.setDataCloudVersion(version);
        input.setTargetEntity(entity);
        input.setFetchOnly(fetchOnly);
        config.setMatchInput(input);
        step.setConfiguration(config);
        ReflectionTestUtils.setField(step, "fuzzyBlockSizeThreshold", 100);
        ReflectionTestUtils.setField(step, "maxNumAccountBlocks", 2);
        ReflectionTestUtils.setField(step, "maxNumContactBlocks", 4);
        ReflectionTestUtils.setField(step, "maxNumTxnBlocks", 8);
        return step;
    }
}
