package com.latticeengines.matchapi.controller;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;

public class AMStatisticsResourceDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    @Test(groups={ "deployment" }, enabled = false)
    public void testGetTopAttrTree() {
        TopNAttributeTree tree = amStatsProxy.getTopAttrTree();
        Assert.assertNotNull(tree);
        // TODO: other assertions
    }

    @Test(groups={ "deployment" }, enabled = false)
    public void testGetTopCube() {
        AccountMasterCube cube = amStatsProxy.getCube(null);
        Assert.assertNotNull(cube);
        // TODO: other assertions
    }
}
