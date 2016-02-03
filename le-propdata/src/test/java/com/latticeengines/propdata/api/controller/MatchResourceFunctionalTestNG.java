package com.latticeengines.propdata.api.controller;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.MatchOutput;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;

public class MatchResourceFunctionalTestNG extends PropDataApiFunctionalTestNGBase {
    private static final String MATCH_ENDPOINT = "propdata/matches";

    @Test(groups = { "api" }, enabled = true)
    public void testPredefined() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] { { "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };

        MatchInput input = MatchResourceTestUtils.prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
    }

}
