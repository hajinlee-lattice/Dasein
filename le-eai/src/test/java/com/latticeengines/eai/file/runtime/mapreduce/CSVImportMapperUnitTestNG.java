package com.latticeengines.eai.file.runtime.mapreduce;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CSVImportMapperUnitTestNG {

    private CSVImportMapper mapper = new CSVImportMapper();

    @Test(groups = "unit")
    public void testParseStringToNumber() {
        String int1 = "1,135.00";
        try {
            Assert.assertEquals(mapper.parseStringToNumber(int1).intValue(), 1135);
        } catch (Exception e) {
            Assert.fail("Should not have thrown exceptions.");
        }
        String double1 = "275,416,298.00";
        try {
            Assert.assertEquals(mapper.parseStringToNumber(double1).doubleValue(), 275416298.00);
        } catch (Exception e) {
            Assert.fail("Should not have thrown exceptions.");
        }
        String long1 = "1,233,445,212,314.00";
        try {
            Assert.assertEquals(mapper.parseStringToNumber(long1).longValue(), 1233445212314l);
        } catch (Exception e) {
            Assert.fail("Should not have thrown exceptions.");
        }
        String float1 = "123,123.01";
        try {
            Assert.assertEquals(mapper.parseStringToNumber(float1).floatValue(), 123123.01f);
        } catch (Exception e) {
            Assert.fail("Should not have thrown exceptions.");
        }
        String double2 = "123,123.01ac";
        try {
            mapper.parseStringToNumber(double2).doubleValue();
            Assert.fail("Should HAVE thrown exceptions.");
        } catch (Exception e) {
            Assert.assertTrue(true, "Should not have thrown exceptions.");
        }
        String double3 = "ac123,123.01";
        try {
            mapper.parseStringToNumber(double3).doubleValue();
            Assert.fail("Should HAVE thrown exceptions.");
        } catch (Exception e) {
            Assert.assertTrue(true, "Should not have thrown exceptions.");
        }
    }
}
