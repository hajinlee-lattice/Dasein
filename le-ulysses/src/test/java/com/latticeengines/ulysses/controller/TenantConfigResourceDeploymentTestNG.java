package com.latticeengines.ulysses.controller;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class TenantConfigResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testGetFeatureFlags() {
        FeatureFlagValueMap map = getOAuth2RestTemplate().getForObject(
                getUlyssesRestAPIPort() + "/ulysses/tenant/featureflags", FeatureFlagValueMap.class);

        assertTrue(map.size() > 0);
    }
}
