package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class Base64UtilsUnitTestNG {

    private static final String STR_TO_ENCRYPT = "latticeengines10@gmail.com";

    @Test(groups = "unit")
    public void testEncodeBase64() {
        String encrypted = Base64Utils.encodeBase64(STR_TO_ENCRYPT, false, Integer.MAX_VALUE);
        System.out.println("Encrypted: " + encrypted);
    }

    @Test(groups = "unit")
    public void testEncodeBase64WithDefaultTrim() {
        String encrypted = Base64Utils.encodeBase64WithDefaultTrim(STR_TO_ENCRYPT);
        System.out.println("Encrypted: " + encrypted);
        Assert.assertEquals(encrypted, "bGF0dGlj");
    }
}
