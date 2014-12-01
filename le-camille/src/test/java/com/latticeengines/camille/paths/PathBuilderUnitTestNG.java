package com.latticeengines.camille.paths;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.Path;

public class PathBuilderUnitTestNG {

    @Test(groups = "unit")
    public void testBuildTenantServicePath() {
        Path p = PathBuilder.buildCustomerSpaceServicePath("podID", "contractID", "tenantID", "spaceID", "serviceName");
        String correct = "/" + PathConstants.PODS + "/podID/" + PathConstants.CONTRACTS + "/contractID/"
                + PathConstants.TENANTS + "/tenantID/" + PathConstants.SPACES + "/spaceID/" + PathConstants.SERVICES
                + "/serviceName";

        String toValidate = p.toString();
        Assert.assertEquals(toValidate, correct);
    }

    @Test(groups = "unit")
    public void testDeepCopy() {
        Path p0 = new Path("/p0");
        Path p1 = new Path("/p0");

        Assert.assertFalse(p0 == p1);
        Assert.assertTrue(p0.equals(p1));
    }
}
