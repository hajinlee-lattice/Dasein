package com.latticeengines.propdata.api.controller;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;

public class MatchResourceFunctionalTestNG extends PropDataApiFunctionalTestNGBase {
    private static final String MATCH_ENDPOINT = "propdata/matches";

    @Test(groups = { "api" }, enabled = true)
    public void testPredefined() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };

        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

}
