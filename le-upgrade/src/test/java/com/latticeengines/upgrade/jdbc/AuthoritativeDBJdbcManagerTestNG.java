package com.latticeengines.upgrade.jdbc;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.domain.BardInfo;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class AuthoritativeDBJdbcManagerTestNG extends UpgradeFunctionalTestNGBase{
    
    @Autowired
    private AuthoritativeDBJdbcManager authoritativeDBJdbcManager;

    @Test(groups = "functional")
    public void testGetAllDeploymentIds() {
        List<String> deploymentIds = authoritativeDBJdbcManager.getDeploymentIDs("1.3.4");
        Assert.assertTrue(deploymentIds.size() > 0, "Found no deploymentId.");
    }

    @Test(groups = "functional")
    public void testGetBardDBInfos() throws Exception {
        List<BardInfo> bardInfos = authoritativeDBJdbcManager.getBardDBInfos("948");
        Assert.assertTrue(bardInfos.size() > 0, "Found no bardInfo.");
        Assert.assertEquals(bardInfos.get(7).getDisplayName(), "Bard DB", "Cannot read Bard DB info");
    }

}
