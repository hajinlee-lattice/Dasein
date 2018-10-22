package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RuleBasedComparatorUnitTestNG {

    @Test(groups = "unit")
    public void testPreferBooleanValuedStringAsTrue() {
        String[] trueValuedStrs = { "Y", "y", "YES", "Yes", "yes", "yeS", "1", "TRUE", "True", "true", "tRue" };
        String[] otherStrs = { "N", "n", "NO", "No", "no", "nO", "0", "FALSE", "False", "false", "faLse", "other strs",
                "   ", "", null };
        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferBooleanValuedStringAsTrue", String.class,
                    String.class);
            verifyStringRuleBasedComparator(trueValuedStrs, otherStrs, testMethod);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferBooleanValuedStringAsTrue", e);
        }

    }

    @Test(groups = "unit")
    public void testNonBlankString() {
        String[] nonBlankStrs = { "Lattice Engines" };
        String[] blankStrs = { null, "", " " };

        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferNonBlankString", String.class,
                    String.class);
            verifyStringRuleBasedComparator(nonBlankStrs, blankStrs, testMethod);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferNonBlankString", e);
        }
    }

    @Test(groups = "unit")
    public void testPreferExpectedString() {
        String expected = "Lattice";
        String[] same = {expected};
        String[] sameInsensitiveCase = { expected, expected.toUpperCase() };
        String[] diff = { expected.toUpperCase(), "Lattice Engines", "", "  ", null };
        String[] diffInsensitiveCase = { "Lattice Engines", "", "  ", null };

        try {
            Method testMethod = RuleBasedComparator.class.getMethod("preferExpectedString", String.class, String.class,
                    String.class, boolean.class);
            // Case sensitive
            verifyStringRuleBasedComparator(same, diff, testMethod, expected, true);
            // Case insensitive
            verifyStringRuleBasedComparator(sameInsensitiveCase, diffInsensitiveCase, testMethod,
                    expected, false);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Fail to invoke test method preferExpectedString", e);
        }
    }

    private void verifyStringRuleBasedComparator(String[] posCases, String[] negCases, Method testMethod,
            Object... otherParas) {
        Object[] paras;
        if (otherParas == null) {
            paras = new Object[2];
        } else {
            paras = new Object[2 + otherParas.length];
            System.arraycopy(otherParas, 0, paras, 2, otherParas.length);
        }
        try {
            for (String posCase : posCases) {
                for (String negCase : negCases) {
                    paras[0] = posCase;
                    paras[1] = negCase;
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), 1);
                    paras[0] = negCase;
                    paras[1] = posCase;
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), -1);
                }
            }
            for (String posCase1 : posCases) {
                for (String posCase2 : posCases) {
                    paras[0] = posCase1;
                    paras[1] = posCase2;
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), 0);
                }
            }
            for (String negCase1 : negCases) {
                for (String negCase2 : negCases) {
                    paras[0] = negCase1;
                    paras[1] = negCase2;
                    Assert.assertEquals(((int) testMethod.invoke(null, paras)), 0);
                }
            }
        } catch (SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException("Fail to invoke test method " + testMethod.getName(), e);
        }

    }
}
