package com.latticeengines.domain.exposed.cdl;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class S3ImportSystemUnitTestNG {

    @Test(groups = "unit")
    public void testJsonString() {
        S3ImportSystem s3ImportSystem = new S3ImportSystem();
        s3ImportSystem.setName("abc");
        s3ImportSystem.setDisplayName("display_abc");
        s3ImportSystem.setSystemType(S3ImportSystem.SystemType.Eloqua);
        s3ImportSystem.setPriority(1);
        String systemJson = JsonUtils.serialize(s3ImportSystem);
        Assert.assertTrue(systemJson.contains("\"is_primary_system\":false"));
        s3ImportSystem.setMapToLatticeAccount(Boolean.TRUE);
        systemJson = JsonUtils.serialize(s3ImportSystem);
        Assert.assertTrue(systemJson.contains("\"is_primary_system\":true"));
    }
}
