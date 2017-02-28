package com.latticeengines.ulysses.controller;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class AccountResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {
    @Test(groups = "deployment", expectedExceptions = Exception.class)
    public void testGetAccounts() {
        FrontEndQuery query = new FrontEndQuery();
        try {
            long count = getOAuth2RestTemplate().postForObject(getUlyssesRestAPIPort() + "/ulysses/accounts/count",
                    query, Long.class);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
