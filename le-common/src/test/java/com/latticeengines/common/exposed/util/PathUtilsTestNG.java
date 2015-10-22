package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PathUtilsTestNG {

    @Test(groups = { "unit", "functional" })
    public void testStripoutProtocal() throws Exception {
        String path = "hdfs://localhost:9000/Pods/Default/Contracts/DemoContract/Tenants/DemoTenant/Spaces/Production/Data/Tables/Lead/Extracts/2015-10-21-22-40-34/Lead-2015-10-21.avro";
        Assert.assertEquals(
                PathUtils.stripoutProtocal(path),
                "/Pods/Default/Contracts/DemoContract/Tenants/DemoTenant/Spaces/Production/Data/Tables/Lead/Extracts/2015-10-21-22-40-34/Lead-2015-10-21.avro");
    }
}
