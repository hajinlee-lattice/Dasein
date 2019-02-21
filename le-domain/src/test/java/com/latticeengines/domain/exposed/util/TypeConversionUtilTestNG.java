package com.latticeengines.domain.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;

public class TypeConversionUtilTestNG {
    private static final Logger log = LoggerFactory.getLogger(TypeConversionUtilTestNG.class);

    @Test(groups = "unit", dataProvider = "batchData")
    public void testTypeConvert(Object data, TypeConvertStrategy strategy, Object expectedOutput) {
        switch (strategy) {
        case ANY_TO_STRING:
            Assert.assertEquals(TypeConversionUtil.toString(data), expectedOutput);
            break;
        case ANY_TO_INT:
            Assert.assertEquals(TypeConversionUtil.toInteger(data), expectedOutput);
            break;
        case STRING_TO_INT:
            Assert.assertEquals(TypeConversionUtil.toInteger(String.valueOf(data)), expectedOutput);
            break;
        case ANY_TO_LONG:
            Assert.assertEquals(TypeConversionUtil.toLong(data), expectedOutput);
            break;
        case STRING_TO_LONG:
            Assert.assertEquals(TypeConversionUtil.toLong(String.valueOf(data)), expectedOutput);
            break;
        case ANY_TO_DOUBLE:
            Assert.assertEquals(TypeConversionUtil.toDouble(data), expectedOutput);
            break;
        case ANY_TO_BOOLEAN:
            Assert.assertEquals(TypeConversionUtil.toBoolean(data), expectedOutput);
            break;
        case STRING_TO_BOOLEAN:
            Assert.assertEquals(TypeConversionUtil.toBoolean(String.valueOf(data)),
                    expectedOutput);
            break;
        default:
            log.info("Strategy not found");
        }
        }

    @DataProvider(name = "batchData")
    private Object[][] provideBatchData() {
        // value, strategy, expectedOutput
        return new Object[][] { //
                // AnyToString
                { "1", TypeConvertStrategy.ANY_TO_STRING, "1" },
                { 1, TypeConvertStrategy.ANY_TO_STRING, "1" },
                { 1L, TypeConvertStrategy.ANY_TO_STRING, "1" },
                { 1F, TypeConvertStrategy.ANY_TO_STRING, "1.0" },
                { 1D, TypeConvertStrategy.ANY_TO_STRING, "1.0" },
                // AnyToLong
                { 6L, TypeConvertStrategy.ANY_TO_LONG, 6L },
                { "2", TypeConvertStrategy.ANY_TO_LONG, 2L },
                { 7D, TypeConvertStrategy.ANY_TO_LONG, 7L },
                { 4F, TypeConvertStrategy.ANY_TO_LONG, 4L },
                { 3, TypeConvertStrategy.ANY_TO_LONG, 3L },
                // AnyToDouble
                { 8D, TypeConvertStrategy.ANY_TO_DOUBLE, 8D },
                { 5L, TypeConvertStrategy.ANY_TO_DOUBLE, 5D },
                { "89", TypeConvertStrategy.ANY_TO_DOUBLE, 89D },
                { 6F, TypeConvertStrategy.ANY_TO_DOUBLE, 6D },
                // AnyToBoolean
                { 0, TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.FALSE },
                { 1L, TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.TRUE },
                { "Y", TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.TRUE },
                { true, TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.TRUE },
                { "YES", TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.TRUE },
                { "TRUE", TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.TRUE },
                { "1", TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.TRUE },
                { "N", TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.FALSE },
                { "NO", TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.FALSE },
                { "FALSE", TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.FALSE },
                { "0", TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.FALSE },
                // AnyToInt
                { 12, TypeConvertStrategy.ANY_TO_INT, 12 },
                { 2F, TypeConvertStrategy.ANY_TO_INT, 2 },
                { true, TypeConvertStrategy.ANY_TO_INT, 1 },
                { 100L, TypeConvertStrategy.ANY_TO_INT, 100 },
                { "2", TypeConvertStrategy.ANY_TO_INT, 2 },
                { 6D, TypeConvertStrategy.ANY_TO_INT, 6 },
                { 3L, TypeConvertStrategy.ANY_TO_INT, 3 } };
    }
}
