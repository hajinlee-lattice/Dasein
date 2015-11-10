package com.latticeengines.dataplatform.service.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;

public class ModelingServiceValidationAspectUnitTestNG {

    @BeforeClass(groups = "unit")
    public void setup() {
    }

    @Test(groups = "unit", dataProvider = "validateNameData")
    public void validateName(String value, boolean result, LedpCode ledpCode) {
        ModelingServiceValidationAspect aop = new ModelingServiceValidationAspect();

        LoadConfiguration config = new LoadConfiguration();
        config.setCustomer(value);
        config.setTable("goodName");
        config.setMetadataTable("goodName");
        assertLoadValidation(result, ledpCode, aop, config);

        config = new LoadConfiguration();
        config.setCustomer("goodName");
        config.setTable(value);
        config.setMetadataTable("goodName");
        assertLoadValidation(result, ledpCode, aop, config);

        config = new LoadConfiguration();
        config.setCustomer("goodName");
        config.setTable("goodName");
        config.setMetadataTable(value);
        assertLoadValidation(result, ledpCode, aop, config);

    }

    @Test(groups = "unit", dataProvider = "validateColumnNames")
    public void validateColumnName(String columnName, boolean result, LedpCode ledpCode) {
        ModelingServiceValidationAspect aop = new ModelingServiceValidationAspect();
        assertColumnName(result, ledpCode, aop, columnName);
    }

    private void assertColumnName(boolean result, LedpCode ledpCode, ModelingServiceValidationAspect aop,
            String columnName) {
        LedpException ex = null;
        try {
            aop.validateColumnName(columnName);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        if (result) {
            Assert.assertTrue(ex == null);
            return;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), ledpCode);
    }

    private void assertLoadValidation(boolean result, LedpCode ledpCode, ModelingServiceValidationAspect aop,
            LoadConfiguration config) {
        LedpException ex = null;
        try {
            aop.validateLoadConfig(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        if (result) {
            Assert.assertTrue(ex == null);
            return;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), ledpCode);
    }

    @DataProvider(name = "validateNameData")
    public static Object[][] getValidateNameData() {
        return new Object[][] { { "goodName", true, null }, //
                { "good_Name", true, null }, //
                { "good.Name", true, null }, //
                { "{badName}", false, LedpCode.LEDP_10007 }, //
                { "[badName]", false, LedpCode.LEDP_10007 }, //
                { "bad/Name", false, LedpCode.LEDP_10007 }, //
                { "bad:Name", false, LedpCode.LEDP_10007 }, //
                { "", false, LedpCode.LEDP_10007 }, //
                { ".", false, LedpCode.LEDP_10007 }, //
                { "..", false, LedpCode.LEDP_10007 }, //
                { "/", false, LedpCode.LEDP_10007 }, //
        };
    }

    @DataProvider(name = "validateColumnNames")
    public static Object[][] getValidateColumnNames() {
        return new Object[][] { { "goodName", true, null }, //
                { "good_Name", true, null }, //
                { "bad Name", false, LedpCode.LEDP_10007 }, //
                { "bad:Name", false, LedpCode.LEDP_10007 }, //
                { "bad/Name", false, LedpCode.LEDP_10007 }, //
                { "bad:Name", false, LedpCode.LEDP_10007 }, //
        };
    }

}
