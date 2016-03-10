package com.latticeengines.propdata.api.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;


public class MatchResourceFunctionalTestNG extends PropDataApiFunctionalTestNGBase {
    private static final String MATCH_ENDPOINT = "propdata/matches/realtime";

    @Test(groups = { "api" })
    public void testPredefined() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 123, "chevron.com", null, null, null, null } };

        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
        Assert.assertFalse(output.getResult().get(0).getInput().isEmpty(), "result record should contain input values");
    }

    @Test(groups = { "api" })
    public void testCachedLocationMatchBulk() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 1, null, "Chevron Corporation", "San Ramon", "California", "USA" },
                { 2, null, "chevron Corporation", "San Ramon", "California", null },
                { 3, null, "chevron corporation", null, null, null },
                { 4, null, "Chevron Corporation", null, null, "USA" },
                { 5, null, "Royal Dutch Shell plc", "The Hague", "South Holland", "Netherlands" },
                { 6, null, "Royal Dutch Shell plc", "The Hague", "South Holland", null }
        };

        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getStatistics().getRowsMatched(), new Integer(6));
    }

    @Test(groups = { "api" }, enabled = true, dataProvider = "cachedMatchGoodDataProvider")
    public void testCachedLocationMatchGood(String name, String city, String state, String country) {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 1, null, name, city, state, country }
        };

        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0, String.format("(%s, %s, %s, %s) gives %d results", name, city,
                state, country, output.getResult().size()));
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0,
                String.format("(%s, %s, %s, %s) gives %d matched", name, city, state, country,
                        output.getStatistics().getRowsMatched()));
    }

    @DataProvider(name = "cachedMatchGoodDataProvider")
    private Object[][] cachedMatchGoodDataProvider() {
        return new Object[][] {
                { "Chevron Corporation", "San Ramon", "California", "USA" },
                { "chevron Corporation", "San Ramon", "California", null },
                { "chevron corporation", null, null, null },
                { "Chevron Corporation", null, null, "USA" },
                { "Royal Dutch Shell plc", "The Hague", "South Holland", "Netherlands" }
        };
    }

    @Test(groups = { "api" }, enabled = true, dataProvider = "cachedMatchBadDataProvider")
    public void testCachedLocationMatchBad(String name, String city, String state, String country) {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] { { 1, null, name, city, state, country } };

        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        List<Object> result = output.getResult().get(0).getOutput();
        Integer notNull = 0;
        for (Object obj: result) {
            if (obj != null) {
                notNull++;
            }
        }
        Assert.assertTrue(notNull == 1, String.format("(%s, %s, %s, %s) gives %d not null result objects",
                name, city, state, country, notNull));
    }

    @DataProvider(name = "cachedMatchBadDataProvider")
    private Object[][] cachedMatchBadDataProvider() {
        return new Object[][] {
                { "chevron corporation", "Nowhere", null, null },
                { "Royal Dutch Shell plc", "The Hague", "South Holland", null }
        };
    }

}
