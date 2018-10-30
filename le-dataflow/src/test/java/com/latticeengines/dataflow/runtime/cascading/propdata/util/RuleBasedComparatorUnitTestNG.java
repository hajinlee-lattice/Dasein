package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.kitesdk.shaded.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class RuleBasedComparatorUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(RuleBasedComparatorUnitTestNG.class);

    @Test(groups = "unit")
    public void testPreferBooleanValuedStringAsTrue() {
        String[][] trueValuedStrs = { //
                { "Y" }, { "y" }, { "YES" }, { "Yes" }, //
                { "yes" }, { "yeS" }, { "1" }, { "TRUE" }, //
                { "True" }, { "true" }, { "tRue" } //
        };
        String[][] otherStrs = { //
                { "N" }, { "n" }, { "NO" }, { "No" }, //
                { "no" }, { "nO" }, { "0" }, { "FALSE" }, //
                { "False" }, { "false" }, { "faLse" }, //
                { "other strs" }, { "   " }, { "" }, { null } //
        };
        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferBooleanValuedStringAsTrue", String.class,
                    String.class);
            verifyAll(trueValuedStrs, otherStrs, testMethod);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferBooleanValuedStringAsTrue", e);
        }

    }

    @Test(groups = "unit")
    public void testNonBlankString() {
        String[][] nonBlankStrs = { { "Lattice Engines" } };
        String[][] blankStrs = { { null }, { "" }, { " " } };

        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferNonBlankString", String.class,
                    String.class);
            verifyAll(nonBlankStrs, blankStrs, testMethod);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferNonBlankString", e);
        }
    }

    @Test(groups = "unit")
    public void testPreferExpectedString() {
        String expected = "Lattice";
        String[][] same = { { expected } };
        String[][] sameInsensitiveCase = { { expected }, { expected.toUpperCase() } };
        String[][] diff = { { expected.toUpperCase() }, { "Lattice Engines" }, { "" }, { "  " }, { null } };
        String[][] diffInsensitiveCase = { { "Lattice Engines" }, { "" }, { "  " }, { null } };

        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferExpectedString", String.class, String.class,
                    String.class, boolean.class);
            log.info("Test Case: case sensitive");
            verifyAll(same, diff, testMethod, expected, false);
            log.info("Test Case: case insensitive");
            verifyAll(sameInsensitiveCase, diffInsensitiveCase, testMethod,
                    expected, true);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferExpectedString", e);
        }
    }

    @Test(groups = "unit")
    public void testPreferEqualStrings() {
        // To test caseInsensitive = false, trim = false
        String[][] equalExactly = { //
                { null, null }, { "", "" }, //
                { " ", " " }, { "AA", "AA" } //
        };
        String[][] notEqualExactly = { //
                { null, "" }, { null, " " }, //
                { " ", "" }, { "AA", "aa" }, //
                { "AA", "BB" } //
        };
        // To test caseInsensitive = true, trim = false
        String[][] equalCaseInsensitive = { //
                { null, null }, { "", "" }, //
                { " ", " " }, { "AA", "AA" }, //
                { "Aa", "aA" }, { "AA", "aa" } //
        };
        String[][] notEqualCaseInsensitive = { //
                { null, "" }, { null, " " }, //
                { " ", "" }, { "AA", "BB" }, //
                { "aa", "AAA" },//
        };
        // To test caseInsensitive = false, trim = true
        String[][] equalTrimmed = { //
                { null, null }, { "", "" }, //
                { " ", " " }, { "AA", "AA" }, //
                { " A A ", "A A" }, { "  ", "" }, //
        };
        String[][] notEqualTrimmed = { //
                { null, "" }, { null, " " }, //
                { "AA", "aa" }, { "AA", "BB" }, //
                { " A A", "AA" }, { "A  A  ", "A A" }, //
        };
        // To test caseInsensitive = true, trim = true
        String[][] equalTrimmedAndCaseInsensitive = {//
                { null, null }, { "", "" }, //
                { " ", "  " }, { "AA", "AA" }, //
                { " Aa ", "aA" }, { "AA  ", "  aa" } //
        };
        String[][] notEqualTrimmedAndCaseInsensitive =  {//
                { null, "" }, { null, " " }, //
                { " A a", "a  A" }, { "AA", "A A" }, //
        };
        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferEqualStrings", String.class, String.class,
                    String.class, String.class, boolean.class, boolean.class);
            log.info("Test Case: exact equal");
            verifyAll(equalExactly, notEqualExactly, testMethod, false, false);
            log.info("Test Case: equal with case insensitive");
            verifyAll(equalCaseInsensitive, notEqualCaseInsensitive, testMethod, true, false);
            log.info("Test Case: equal after trimmed");
            verifyAll(equalTrimmed, notEqualTrimmed, testMethod, false, true);
            log.info("Test Case: equal after trimmed and with case insensitive");
            verifyAll(equalTrimmedAndCaseInsensitive, notEqualTrimmedAndCaseInsensitive, testMethod, true, true);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferEqualStrings", e);
        }
    }

    @Test(groups = "unit")
    public void testPreferLargerLongWithThreshold() {
        // TODO: Currently test cases are designed targeting at use case of
        // comparing sales volume
        // So didn't cover general combinations of threshold and gap
        // If wants to extend use case / compare abs etc, need to further
        // improve both utils method and test cases
        long threshold = 100L;
        long gap = 10L;
        // candidate, compareTo, expected
        Object[][] testCases = new Object[][] { //
                // candidate >= threshold, compareTo >= threshold
                { 111L, 100L, 1 }, //
                // candidate >= threshold, compareTo < threshold
                { 100L, 89L, 1 }, //
                // candidate >= threshold, compareTo is null
                { 111L, null, 1 }, //

                // Reverse of cases above
                { 100L, 111L, -1 }, //
                { 99L, 110L, -1 }, //
                { null, 111L, -1 }, //

                // both candidate and compareTo >= threshold, difference <= gap
                { 111L, 101L, 0 }, //
                { 101L, 111L, 0 }, //
                // one >= threshold, one < threshold, difference <= gap
                { 100L, 99L, 0 }, //
                { 99L, 100L, 0 }, //
                // both < threshold
                { 99L, 89L, 0 }, //
                { 99L, 88L, 0 }, //
                { 89L, 99L, 0 }, //
                { 88L, 99L, 0 }, //
                // null (not-null < threshold)
                { 99L, null, 0 }, //
                { null, 99L, 0 }, //
                { null, null, 0 }, //
        };
        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferLargerLongWithThreshold", Long.class,
                    Long.class, long.class, long.class);
            verifyInPairs(testCases, testMethod, threshold, gap);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferLargerLongWithThreshold", e);
        }
    }

    @Test(groups = "unit")
    public void testPreferLargerLong() {
        // candidate, compareTo, expected
        Object[][] testCases = new Object[][] { //
                { 1000L, 999L, 1 }, //
                { 1000L, null, 1 }, //

                // reverse of cases above
                { 999L, 1000L, -1 }, //
                { null, 999L, -1 }, //

                { 1000L, 1000L, 0 }, //
                { null, null, 0 }, //
        };
        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferLargerLong", Long.class, Long.class);
            verifyInPairs(testCases, testMethod);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferLargerLong", e);
        }
    }

    @Test(groups = "unit")
    public void testLargerInteger() {
        // candidate, compareTo, expected
        Object[][] testCases = new Object[][] { //
                { 1000, 999, 1 }, //
                { 1000, null, 1 }, //

                // reverse of cases above
                { 999, 1000, -1 }, //
                { null, 999, -1 }, //

                { 1000, 1000, 0 }, //
                { null, null, 0 }, //
        };
        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferLargerInteger", Integer.class,
                    Integer.class);
            verifyInPairs(testCases, testMethod);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferLargerInteger", e);
        }
    }

    @Test(groups = "unit")
    public void testSmallerInteger() {
        // candidate, compareTo, expected
        Object[][] testCases = new Object[][] { //
                { 999, 1000, 1 }, //
                { 999, null, 1 }, //

                // reverse of cases above
                { 1000, 999, -1 }, //
                { null, 999, -1 }, //

                { 1000, 1000, 0 }, //
                { null, null, 0 }, //
        };
        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferSmallerInteger", Integer.class,
                    Integer.class);
            verifyInPairs(testCases, testMethod);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferSmallerInteger", e);
        }
    }

    /**
     * Expectation: 
     * Compare any from posCases with any from negCases, always posCase wins 
     * Compare any from posCases between each other, always tie
     * Compare any from negCases between eath other, always tie
     * 
     * 1st dimension of case array is number of test case - posCases and
     * negCases could have different number of test cases
     * 
     * 2nd dimention of case array is number of candidates needed by the method
     * 
     * eg. 
     * To test preferNonBlankString, need to pass in Object[n][1] for both posCases and negCases 
     * To test preferEqualStrings, need to pass in Object[n][2] for both posCases and negCases
     * 
     * @param posCases
     * @param negCases
     * @param testMethod
     * @param otherParas
     */
    private void verifyAll(@NotNull Object[][] posCases, @NotNull Object[][] negCases, @NotNull Method testMethod,
            Object... otherParas) {
        Preconditions.checkNotNull(testMethod);
        Preconditions.checkArgument(ArrayUtils.isNotEmpty(posCases));
        Preconditions.checkArgument(ArrayUtils.isNotEmpty(negCases));
        int candidateNum = -1;
        for (Object[] pc : posCases) {
            Preconditions.checkArgument(ArrayUtils.isNotEmpty(pc));
            if (candidateNum == -1) {
                candidateNum = pc.length;
            } else {
                Preconditions.checkArgument(pc.length == candidateNum);
            }
        }
        for (Object[] nc : negCases) {
            Preconditions.checkArgument(ArrayUtils.isNotEmpty(nc));
            Preconditions.checkArgument(nc.length == candidateNum);
        }

        final int canNum = candidateNum;
        Object[] paras;
        if (otherParas == null) {
            paras = new Object[candidateNum * 2];
        } else {
            paras = new Object[candidateNum * 2 + otherParas.length];
            System.arraycopy(otherParas, 0, paras, candidateNum * 2, otherParas.length);
        }

        // Compare positive case with negative case, positive case should beat
        // negative case
        Stream.of(posCases).forEach(pc -> {
            Stream.of(negCases).forEach(ng -> {
                IntStream.range(0, canNum).forEach(i -> {
                    paras[i] = pc[i];
                    paras[i + canNum] = ng[i];
                });
                try {
                    log.info("Invoking method " + testMethod.getName() + " with paras: " + Arrays.toString(paras));
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), 1);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new RuntimeException("Fail to invoke test method " + testMethod.getName(), e);
                }
                IntStream.range(0, canNum).forEach(i -> {
                    paras[i] = ng[i];
                    paras[i + canNum] = pc[i];
                });
                try {
                    log.info("Invoking method " + testMethod.getName() + " with paras: " + Arrays.toString(paras));
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), -1);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new RuntimeException("Fail to invoke test method " + testMethod.getName(), e);
                }

            });
        });

        // Compare positive case with positive case, got tie
        Stream.of(posCases).forEach(pc1 -> {
            Stream.of(posCases).forEach(pc2 -> {
                IntStream.range(0, canNum).forEach(i -> {
                    paras[i] = pc1[i];
                    paras[i + canNum] = pc2[i];
                });
                try {
                    log.info("Invoking method " + testMethod.getName() + " with paras: " + Arrays.toString(paras));
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), 0);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new RuntimeException("Fail to invoke test method " + testMethod.getName(), e);
                }
                IntStream.range(0, canNum).forEach(i -> {
                    paras[i] = pc2[i];
                    paras[i + canNum] = pc1[i];
                });
                try {
                    log.info("Invoking method " + testMethod.getName() + " with paras: " + Arrays.toString(paras));
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), 0);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new RuntimeException("Fail to invoke test method " + testMethod.getName(), e);
                }

            });
        });

        // Compare negative case with negative case, got tie
        Stream.of(negCases).forEach(ng1 -> {
            Stream.of(negCases).forEach(ng2 -> {
                IntStream.range(0, canNum).forEach(i -> {
                    paras[i] = ng1[i];
                    paras[i + canNum] = ng2[i];
                });
                try {
                    log.info("Invoking method " + testMethod.getName() + " with paras: " + Arrays.toString(paras));
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), 0);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new RuntimeException("Fail to invoke test method " + testMethod.getName(), e);
                }
                IntStream.range(0, canNum).forEach(i -> {
                    paras[i] = ng2[i];
                    paras[i + canNum] = ng1[i];
                });
                try {
                    log.info("Invoking method " + testMethod.getName() + " with paras: " + Arrays.toString(paras));
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), 0);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    throw new RuntimeException("Fail to invoke test method " + testMethod.getName(), e);
                }

            });
        });

    }

    /**
     * Expectation: 
     * There is no definitely winner or loser
     * Win or lose is relative depending on candidate and compareTo
     * For testing methods like largerXXX, smallerXXX etc
     * 
     * 1st dimension of testCases is number of test cases
     * 2nd dimension of testCases should be:
     * candidate1, candidate2, ..., compareTo1, compareTo2, ..., expected result(boolean)
     * Number of candidates and number of compareTo should be same
     * 
     * @param posCases
     * @param negCases
     * @param testMethod
     * @param otherParas
     */
    private void verifyInPairs(@NotNull Object[][] testCases, @NotNull Method testMethod, Object... otherParas) {
        Preconditions.checkNotNull(testMethod);
        Preconditions.checkArgument(ArrayUtils.isNotEmpty(testCases));
        // number of candidate plus number of compareTo needed by test method
        int candidatePlusCompareTo = -1;
        Set<Integer> expectedRes = new HashSet<>(Arrays.asList(-1, 0, 1));
        for (Object[] testCase : testCases) {
            Preconditions.checkArgument(ArrayUtils.isNotEmpty(testCase));
            Preconditions.checkArgument(testCase.length > 1);
            Preconditions.checkArgument(testCase.length % 2 == 1);
            Preconditions.checkArgument(testCase[testCase.length - 1] instanceof Integer);
            Preconditions.checkArgument(expectedRes.contains((Integer) testCase[testCase.length - 1]));
            if (candidatePlusCompareTo == -1) {
                candidatePlusCompareTo = testCase.length - 1;
            } else {
                Preconditions.checkArgument(candidatePlusCompareTo == testCase.length - 1);
            }
        }

        final int canPlusComp = candidatePlusCompareTo;
        Object[] paras;
        if (otherParas == null) {
            paras = new Object[canPlusComp];
        } else {
            paras = new Object[canPlusComp + otherParas.length];
            System.arraycopy(otherParas, 0, paras, canPlusComp, otherParas.length);
        }

        Stream.of(testCases).forEach(testCase -> {
            System.arraycopy(testCase, 0, paras, 0, canPlusComp);
            try {
                log.info("Invoking method " + testMethod.getName() + " with paras: " + Arrays.toString(paras));
                Assert.assertEquals(((int) testMethod.invoke(null, paras)), (int) testCase[testCase.length - 1]);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new RuntimeException("Fail to invoke test method " + testMethod.getName(), e);
            }
        });
    }
}
