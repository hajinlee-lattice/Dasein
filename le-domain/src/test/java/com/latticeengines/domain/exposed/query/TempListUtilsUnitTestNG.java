package com.latticeengines.domain.exposed.query;

import java.time.LocalDate;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TempListUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testParseDate() {
        String tableName = TempListUtils.newTempTableName();
        LocalDate date = TempListUtils.parseDateFromTableName(tableName);
        Assert.assertNotNull(date);
    }

}
