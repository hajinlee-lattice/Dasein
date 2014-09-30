package com.latticeengines.camille.paths;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.Path;

public class PathBuilderUnitTestNG {

    @Test(groups = "unit")
    public void testBuildTenantServicePath() {
        Path p = PathBuilder.buildTenantServicePath("podID", "contractID", "tenantID", "spaceID", "serviceName");
        String correct = DirectoryConstants.PODS + "/podID/" + DirectoryConstants.CONTRACTS + "/contractID/"
                + DirectoryConstants.TENANTS + "/tenantID/" + DirectoryConstants.SPACES + "/spaceID/"
                + DirectoryConstants.SERVICES + "/serviceName";

        String toValidate = p.toString();
        Assert.assertEquals(toValidate, correct);
    }

}
