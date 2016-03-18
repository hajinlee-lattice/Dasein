package com.latticeengines.propdata.api.controller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;
import com.latticeengines.propdata.match.testframework.TestMatchInputUtils;

import edu.emory.mathcs.backport.java.util.Collections;


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
    public void testPublicDomain() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 123, "gmail.com", null, null, null, null } };

        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
        Assert.assertFalse(output.getResult().get(0).getInput().isEmpty(), "result record should contain input values");
        Assert.assertFalse(output.getResult().get(0).getOutput().isEmpty(), "result record should contain result list");
    }

    @Test(groups = { "api" })
    public void testBadDomain() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 123, "notexists123454321fadsfsdacv.com", null, null, null, null } };

        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() == 0);
        Assert.assertFalse(output.getResult().get(0).getInput().isEmpty(), "result record should contain input values");
        Assert.assertNull(output.getResult().get(0).getOutput(), "result record should not contain result list");
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
        Assert.assertEquals(output.getStatistics().getRowsMatched(), new Integer(5));
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
        Assert.assertFalse(output.getResult().get(0).isMatched(),
                String.format("(%s, %s, %s, %s) should not be matched.", name, city, state, country));
    }

    @DataProvider(name = "cachedMatchBadDataProvider")
    private Object[][] cachedMatchBadDataProvider() {
        return new Object[][] {
                { "chevron corporation", "Nowhere", null, null },
                { "Royal Dutch Shell plc", "The Hague", "South Holland", null }
        };
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "api")
    public void testNull() {
        Tenant tenant = new Tenant("PD_Test");
        List<Object> row = Arrays.<Object>asList("Syntel", "prafulla_bhangale@syntelinc.com");
        List<List<Object>> data = Collections.singletonList(row);

        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Email"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));

        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(tenant);
        matchInput.setFields(Arrays.asList("CompanyName", "Email"));
        matchInput.setData(data);
        matchInput.setKeyMap(keyMap);
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.Model);

        String url = getRestAPIHostPort() + MATCH_ENDPOINT;
        MatchOutput output = restTemplate.postForObject(url, matchInput, MatchOutput.class);
        Assert.assertNotNull(output);
    }

}
