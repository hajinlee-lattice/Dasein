package com.latticeengines.eai.runtime.service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

public class EaiRuntimeServiceUnitTestNG {

    @Test(groups = "unit")
    public void testTimeFormat() {
        Date now = new Date();
        String strWithMillis = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss.SSS").format(now);
        String strWithoutMillis = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(now);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            long timeWithMillis = f.parse(strWithMillis).getTime();
            long timeWithoutMillis = f.parse(strWithoutMillis).getTime();
            Assert.assertEquals(timeWithMillis, timeWithoutMillis);
        } catch (ParseException e) {
            Assert.fail("Should not have exception!");
        }
    }
}
