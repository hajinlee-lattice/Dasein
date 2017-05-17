package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MultiListCrossProductUnitTestNG {
    MultiListCrossProductUtil crossUtil;

    public MultiListCrossProductUnitTestNG() {
        crossUtil = new MultiListCrossProductUtil();
    }

    @Test
    public void testCrossProductWithTwoDimensions() {
        List<List<Long>> inputLists = new ArrayList<>();
        inputLists.add(Arrays.asList(5L, 2L));
        inputLists.add(Arrays.asList(10L, 9L));

        List<List<Long>> expectedResult = new ArrayList<>();
        expectedResult.add(Arrays.asList(5L, 10L));
        expectedResult.add(Arrays.asList(5L, 9L));
        expectedResult.add(Arrays.asList(2L, 10L));
        expectedResult.add(Arrays.asList(2L, 9L));

        List<List<Long>> result = crossUtil.calculateCross(inputLists);
        Assert.assertNotNull(result);

        for (List<Long> row : result) {
            checkInExpectedResult(expectedResult, row);
        }
        Assert.assertEquals(result.size(), 4);

        // make sure that first entry in the result contains first
        // elements from all the lists, this allows us to easily disregard
        // lowest most dimension combination in stats rollup logic

        for (int i = 0; i < inputLists.size(); i++) {
            Assert.assertEquals(result.get(0).get(i), inputLists.get(i).get(0));
        }
    }

    @Test
    public void testCrossProductWithThreeDimensions() {
        List<List<Long>> inputLists = new ArrayList<>();
        inputLists.add(Arrays.asList(5L, 2L, 1L));
        inputLists.add(Arrays.asList(10L, 9L));
        inputLists.add(Arrays.asList(20L, 18L));

        List<List<Long>> expectedResult = new ArrayList<>();
        expectedResult.add(Arrays.asList(5L, 10L, 20L));
        expectedResult.add(Arrays.asList(5L, 10L, 18L));
        expectedResult.add(Arrays.asList(5L, 9L, 20L));
        expectedResult.add(Arrays.asList(5L, 9L, 18L));
        expectedResult.add(Arrays.asList(2L, 10L, 20L));
        expectedResult.add(Arrays.asList(2L, 10L, 18L));
        expectedResult.add(Arrays.asList(2L, 9L, 20L));
        expectedResult.add(Arrays.asList(2L, 9L, 18L));
        expectedResult.add(Arrays.asList(1L, 10L, 20L));
        expectedResult.add(Arrays.asList(1L, 10L, 18L));
        expectedResult.add(Arrays.asList(1L, 9L, 20L));
        expectedResult.add(Arrays.asList(1L, 9L, 18L));

        List<List<Long>> result = crossUtil.calculateCross(inputLists);
        Assert.assertNotNull(result);

        for (List<Long> row : result) {
            checkInExpectedResult(expectedResult, row);
        }
        Assert.assertEquals(result.size(), 12);

        // make sure that first entry in the result contains first
        // elements from all the lists, this allows us to easily disregard
        // lowest most dimension combination in stats rollup logic

        for (int i = 0; i < inputLists.size(); i++) {
            Assert.assertEquals(result.get(0).get(i), inputLists.get(i).get(0));
        }
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testCrossProductWithTwoDimensionsWithDuplicateVal() {
        List<List<Long>> inputLists = new ArrayList<>();
        inputLists.add(Arrays.asList(5L, 2L));
        inputLists.add(Arrays.asList(10L, 5L));

        crossUtil.calculateCross(inputLists);
    }

    private void checkInExpectedResult(List<List<Long>> expectedResult, List<Long> row) {
        boolean matchFound = false;

        for (List<Long> expectedRow : expectedResult) {
            Assert.assertEquals(expectedRow.size(), row.size());
            int matchCount = 0;
            for (int i = 0; i < row.size(); i++) {
                if (expectedRow.get(i).equals(row.get(i))) {
                    matchCount++;
                } else {
                    break;
                }
            }

            if (matchCount == row.size()) {
                matchFound = true;
                break;
            }
        }

        Assert.assertTrue(matchFound);
    }
}
