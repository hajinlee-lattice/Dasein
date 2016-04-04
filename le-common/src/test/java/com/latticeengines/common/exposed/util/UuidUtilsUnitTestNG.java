package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertTrue;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UuidUtilsUnitTestNG {

    private String path = "/user/s-analytics/customers/Nutanix_PLS132/models/Q_PLS_Modeling_Nutanix_PLS132/5d074f72-c8f0-4d53-aebc-912fb066daa0/1416355548818_20011";

    private static final String UUID = "5d074f72-c8f0-4d53-aebc-912fb066daa0";

    private static final String MODEL_GUID = "ms__5d074f72-c8f0-4d53-aebc-912fb066daa0-PLSModel";

    private static final String INCORRECT_FORMATTED_ID = "someRamdomString";

    @Test(groups = { "unit", "functional" })
    public void testParseUuid() throws Exception {
        String uuid = UuidUtils.parseUuid(path);
        Assert.assertEquals(uuid, UUID);
    }

    @Test(groups = { "unit", "functional" })
    public void testExtractUuid() throws Exception {
        String uuid = UuidUtils.extractUuid(MODEL_GUID);
        Assert.assertEquals(uuid, UUID);
    }

    @Test(groups = "unit")
    public void parseIncorrectFormattedModelId() {
        try {
            UuidUtils.extractUuid(INCORRECT_FORMATTED_ID);
            Assert.fail("Should have thrown an exception.");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }
}
