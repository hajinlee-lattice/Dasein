package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BitTransferUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testTransfer() {
        Assert.assertEquals(BitTransferUtils.formatSize(1200000), "1.1 MB");
        Assert.assertEquals(BitTransferUtils.formatSize(120), "120 B");
        Assert.assertEquals(BitTransferUtils.formatSize(12000), "11.7 KB");
        Assert.assertEquals(BitTransferUtils.formatSize(1200000000), "1.1 GB");
    }
}
