package com.latticeengines.sqoop.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class YarnConfigurationUtilsUnitTestNG {

    @Test(groups = "unit")
    public void test() {
        String defaultFs = "hdfs://ip-10-141-11-231.lattice.local:8020";

        String address = YarnConfigurationUtils.parseAddress(defaultFs);
        Assert.assertEquals(address, "ip-10-141-11-231.lattice.local");

        String ip = YarnConfigurationUtils.parseMasterIp(address);
        Assert.assertEquals(ip, "10.141.11.231");
    }

}
