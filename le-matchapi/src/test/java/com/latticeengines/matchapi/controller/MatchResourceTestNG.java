package com.latticeengines.matchapi.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.converter.KryoHttpMessageConverter;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.MatchapiFunctionalTestNGBase;
import com.latticeengines.matchapi.testframework.TestMatchInputService;

@Component
public class MatchResourceTestNG extends MatchapiFunctionalTestNGBase {

    private static final String MATCH_ENDPOINT = "/match/matches/realtime";

    private static Logger log = LoggerFactory.getLogger(MatchResourceTestNG.class);

    @Autowired
    private TestMatchInputService testMatchInputService;

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Value("${common.le.stack}")
    private String leStack;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String RELAX_PUBLIC_DOMAIN_CHECK = "RelaxPublicDomainCheck";

    @BeforeClass(groups = { "functional" })
    public void setup() {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();
            camille.upsert(
                    PathBuilder.buildServicePath(podId, PROPDATA_SERVICE, leStack).append(RELAX_PUBLIC_DOMAIN_CHECK),
                    new Document("true"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(groups = { "functional" })
    public void testPredefined() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 123, "chevron.com", null, null, null, null } };

        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        restTemplate.setMessageConverters(Arrays.asList( //
                new MappingJackson2HttpMessageConverter(), //
                new KryoHttpMessageConverter()));
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(KryoHttpMessageConverter.KRYO));
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<MatchInput> entity = new HttpEntity<>(input, headers);
        ResponseEntity<MatchOutput> response = restTemplate.exchange(url, HttpMethod.POST, entity, MatchOutput.class);
        MatchOutput output = response.getBody();
        try (PerformanceTimer timer = new PerformanceTimer("Match via json")) {
            ResponseEntity<MatchOutput> response1 = restTemplate.exchange(url, HttpMethod.POST, entity, MatchOutput.class);
            MatchOutput output1 = response1.getBody();
            Assert.assertNotNull(output1);
        }
        try (PerformanceTimer timer = new PerformanceTimer("Match via kryo")) {
            ResponseEntity<MatchOutput> response1 = restTemplate.exchange(url, HttpMethod.POST, entity, MatchOutput.class);
            MatchOutput output1 = response1.getBody();
            Assert.assertNotNull(output1);
        }

        // MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
        Assert.assertFalse(output.getResult().get(0).getInput().isEmpty(), "result record should contain input values");
    }

    // Public domain comes without name or duns is treated as normal domain if
    // feature flag RelaxPublicDomainCheck in zk is set as true
    @Test(groups = { "functional" })
    public void testPublicDomain() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 123, "gmail.com", "Fake Name", null, null, null } };

        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertEquals(output.getStatistics().getRowsMatched(), new Integer(0));
        Assert.assertFalse(output.getResult().get(0).getInput().isEmpty(), "result record should contain input values");
        Assert.assertFalse(output.getResult().get(0).getOutput().isEmpty(), "result record should contain result list");
    }

    @Test(groups = { "functional" })
    public void testBadDomain() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 123, "notexists123454321fadsfsdacv.com", null, null, null, null } };

        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() == 0);
        Assert.assertFalse(output.getResult().get(0).getInput().isEmpty(), "result record should contain input values");
        Assert.assertFalse(output.getResult().get(0).isMatched(), "result record should be marked as not matched");
    }

    @Test(groups = { "functional" })
    public void testCachedLocationMatchBulk() {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 1, null, "Chevron Corporation", "San Ramon", "California", "USA" },
                { 2, null, "chevron Corporation", "San Ramon", "California", null },
                { 3, null, "chevron corporation", null, null, null },
                { 4, null, "Chevron Corporation", null, null, "USA" },
                { 5, null, "Royal Dutch Shell plc", null, "South Holland", "Netherlands" }
        };

        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        input.setUseRemoteDnB(false);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        for (OutputRecord result : output.getResult()) {
            log.info(String.format("Record %d matched: %b", result.getRowNumber(), result.isMatched()));
        }
        Assert.assertEquals(output.getStatistics().getRowsMatched(), new Integer(5));
    }

    @Test(groups = { "functional" }, dataProvider = "cachedMatchGoodDataProvider", dependsOnMethods = "testLocationEnrichment")
    public void testCachedLocationMatchGood(String name, String city, String state, String country) {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 1, null, name, city, state, country }
        };

        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        input.setUseRemoteDnB(false);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        if (StringUtils.isNotEmpty(state)) {
            Assert.assertTrue(output.getResult().size() > 0, String.format("(%s, %s, %s, %s) should not give %d results", name, city,
                    state, country, output.getResult().size()));
            Assert.assertTrue(output.getStatistics().getRowsMatched() > 0,
                    String.format("(%s, %s, %s, %s) gives %d matched", name, city, state, country,
                            output.getStatistics().getRowsMatched()));
        }
    }

    @Test(groups = { "functional" }, dataProvider = "cachedMatchGoodDataProvider")
    public void testLocationEnrichment(String name, String city, String state, String country) {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] {
                { 1, null, name, city, state, country }
        };

        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        input.setPredefinedSelection(null);
        input.setLogLevelEnum(Level.DEBUG);
        input.setUseRemoteDnB(true);
        input.setCustomSelection(testMatchInputService.enrichmentSelection());
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        if (StringUtils.isNotEmpty(state)) {
            Assert.assertTrue(output.getResult().size() > 0, String.format("(%s, %s, %s, %s) should not give %d results", name, city,
                    state, country, output.getResult().size()));
            Assert.assertTrue(output.getStatistics().getRowsMatched() > 0,
                    String.format("(%s, %s, %s, %s) gives %d matched", name, city, state, country,
                            output.getStatistics().getRowsMatched()));
        }
    }

    @DataProvider(name = "cachedMatchGoodDataProvider")
    private Object[][] cachedMatchGoodDataProvider() {
        return new Object[][] {
                { "Chevron Corporation", "San Ramon", "California", "USA" },
                { "chevron Corporation", "San Ramon", "California", null },
                { "chevron corporation", null, null, null },
                { "Chevron Corporation", null, null, "USA" },
                { "Royal Dutch Shell plc", null, "South Holland", "Netherlands" }
        };
    }

    @Test(groups = { "functional" }, dataProvider = "cachedMatchBadDataProvider")
    public void testCachedLocationMatchBad(String name, String city, String state, String country) {
        String url = getRestAPIHostPort() + MATCH_ENDPOINT;

        Object[][] data = new Object[][] { { 1, null, name, city, state, country } };

        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        MatchOutput output = restTemplate.postForObject(url, input, MatchOutput.class);
        Assert.assertNotNull(output);
        OutputRecord outputRecord = output.getResult().get(0);
        Assert.assertFalse(outputRecord.isMatched(),
                String.format("(%s, %s, %s, %s) should not be matched. But matched to %s",
                        name, city, state, country, outputRecord.getOutput().toString()));
    }

    @DataProvider(name = "cachedMatchBadDataProvider")
    private Object[][] cachedMatchBadDataProvider() {
        return new Object[][] {
                { "impossible name", "Nowhere", null, null }
        };
    }

    @Test(groups = "functional")
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
        matchInput.setPredefinedSelection(Predefined.Model);
        String latestVersion = dataCloudVersionEntityMgr.currentApprovedVersion().getVersion();
        matchInput.setDataCloudVersion(latestVersion);

        String url = getRestAPIHostPort() + MATCH_ENDPOINT;
        MatchOutput output = restTemplate.postForObject(url, matchInput, MatchOutput.class);
        Assert.assertNotNull(output);
    }

}
