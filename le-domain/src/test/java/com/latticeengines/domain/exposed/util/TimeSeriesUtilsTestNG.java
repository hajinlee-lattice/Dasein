package com.latticeengines.domain.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TimeSeriesUtilsTestNG {

    @Test(groups = "unit", dataProvider = "PeriodFileNames")
    public void testGetPeriodFromFileName(String fileName, Integer expectedPeriodId) {
        Assert.assertEquals(TimeSeriesUtils.getPeriodFromFileName(fileName), expectedPeriodId);
    }

    // Schema: fileName, expected periodId
    @DataProvider(name = "PeriodFileNames")
    protected Object[][] providePeriodFileNames() {
        return new Object[][] { //
                { "Period-47877-data.avro", 47877 }, //
                { "Period--47877-data.avro", -47877 }, //
                { "hdfs://QACLUSTER2/Pods/QA/Contracts/QA_CDL_Auto_Demo_0916/Tenants/QA_CDL_Auto_Demo_0916/Spaces/Production/Data/Tables/ConsolidatedRawTransaction_2018_10_14_12_04_09_UTC/Period-47877-data.avro",
                        47877 }, //
                { "hdfs://QACLUSTER2/Pods/QA/Contracts/QA_CDL_Auto_Demo_0916/Tenants/QA_CDL_Auto_Demo_0916/Spaces/Production/Data/Tables/ConsolidatedRawTransaction_2018_10_14_12_04_09_UTC/Period--47877-data.avro",
                        -47877 }
        };
    }
}
