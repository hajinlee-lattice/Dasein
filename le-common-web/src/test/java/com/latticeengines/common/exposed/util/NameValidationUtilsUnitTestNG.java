package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NameValidationUtilsUnitTestNG {

    @Test(groups = { "unit"})
    public void testValidateModelName() throws Exception {
        Assert.assertTrue(NameValidationUtils.validateModelName("ms__4192dfb1-d78c-4521-80d5-cebf477b2978-Rons_Mo"));
        Assert.assertFalse(NameValidationUtils.validateColumnName("ms__4192dfb1-d78c-4521-80d5-cebf477b2978-Ron’s_Mo"));
    }

    @Test(groups = { "unit"})
    public void testValidateColumnName() throws Exception {
        Assert.assertTrue(NameValidationUtils.validateColumnName("Has_Downloaded"));
        Assert.assertFalse(NameValidationUtils.validateColumnName("Has-Downloaded"));
        Assert.assertFalse(NameValidationUtils.validateColumnName("Has北京Download"));
    }
}
