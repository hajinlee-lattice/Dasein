package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.exposed.util.TestDunsGuideBookUtils;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.datacloud.match.testframework.TestMatchInputUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component
public class RealTimeMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RealTimeMatchServiceImplTestNG.class);
    private Camille camille;
    private String podId;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String RELAX_PUBLIC_DOMAIN_CHECK = "RelaxPublicDomainCheck";
    private static final String DECISION_GRAPH_WITHOUT_GUIDE_BOOK = "IceCreamSandwich";
    private static final String DECISION_GRAPH_WITH_GUIDE_BOOK = "JellyBean";

    @Value("${common.le.stack}")
    private String leStack;

    @Autowired
    private RealTimeMatchService realTimeMatchService;

    @Autowired
    private TestMatchInputService testMatchInputService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Autowired
    private DnBCacheService dnbCacheService;

    @Autowired
    private CountryCodeService countryCodeService;

    // Test against retired V1.0 matcher -- DerivedColumnsCache
    // Disable the test as SQL Server is shutdown
    @Test(groups = "functional", enabled = false)
    public void testSimpleMatchRTSCache() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" },
                { 456, "abc@gmail.com", "TestPublicDomainNoState", null, null, "USA" } };
        MatchInput input = testMatchInputService.prepareSimpleRTSMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    // Test against retired V1.0 matcher -- DerivedColumnsCache
    // Disable the test as SQL Server is shutdown
    @Test(groups = "functional", enabled = false)
    public void testSimpleRealTimeBulkMatchRTSCache() {
        Object[][] data = new Object[][] { //
                { 123, null, "Chevron Corporation", null, null, "USA" },
                { 456, null, "Chevron Corporation", "San Ramon", "California", "USA" },
                { 789, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        List<MatchInput> inputs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            MatchInput input = testMatchInputService.prepareSimpleRTSMatchInput(data);
            inputs.add(input);
        }
        BulkMatchInput bulkMatchInput = new BulkMatchInput();
        bulkMatchInput.setInputList(inputs);
        BulkMatchOutput bulkMatchOutput = realTimeMatchService.matchBulk(bulkMatchInput);
        Assert.assertNotNull(bulkMatchOutput);
        Assert.assertEquals(bulkMatchOutput.getOutputList().size(), 50);

        bulkMatchOutput.getOutputList().forEach(output -> {
            Assert.assertNotNull(output.getResult());
            Assert.assertEquals(output.getResult().size(), 3);
        });

        // Test correctness by checking matched domain
        data = new Object[][] { //
                { 1, null, "Facebook Inc", "Menlo Park", "California", "USA" },
                { 2, null, "Alphabet Inc", "Mountain View", "California", "USA" },
                { 3, null, "Amazon.com, Inc.", "Seattle", "Washington", "USA" } };
        inputs.clear();
        for (int i = 0; i < 50; i++) {
            MatchInput input = testMatchInputService.prepareSimpleRTSMatchInput(data);
            inputs.add(input);
        }
        bulkMatchOutput = realTimeMatchService.matchBulk(bulkMatchInput);
        Assert.assertNotNull(bulkMatchOutput);
        Assert.assertEquals(bulkMatchOutput.getOutputList().size(), 50);
        bulkMatchOutput.getOutputList().forEach(output -> {
            Assert.assertNotNull(output.getResult());
            Assert.assertEquals(output.getResult().size(), 3);
            Assert.assertEquals(output.getResult().get(0).getMatchedDomain(), "facebook.com");
            Assert.assertEquals(output.getResult().get(1).getMatchedDomain(), "google.com");
            Assert.assertEquals(output.getResult().get(2).getMatchedDomain(), "amazon.com");
        });
    }

    @Test(groups = "functional")
    public void testSimpleMatchAccountMaster() {
        // Schema: ID, Domain, CompanyName, City, State, Country
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" }, //
                { 456, "testfakedomain.com", null, null, null, null }, //
        };
        // ColumnSelection is RTS
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
        Assert.assertTrue(output.getResult().get(0).isMatched());
        Assert.assertFalse(output.getResult().get(1).isMatched());

        // Test default datacloud version
        input.setDataCloudVersion(null);
        output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
        Assert.assertTrue(output.getResult().get(0).isMatched());
        Assert.assertFalse(output.getResult().get(1).isMatched());
    }

    @Test(groups = "functional")
    public void testSimpleRealTimeBulkMatchAccountMaster() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        List<MatchInput> inputs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
            inputs.add(input);
        }
        BulkMatchInput bulkMatchInput = new BulkMatchInput();
        bulkMatchInput.setInputList(inputs);
        BulkMatchOutput bulkMatchOutput = realTimeMatchService.matchBulk(bulkMatchInput);
        Assert.assertNotNull(bulkMatchOutput);
        Assert.assertEquals(bulkMatchOutput.getOutputList().size(), 50);

        MatchOutput output = bulkMatchOutput.getOutputList().get(0);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), 1);
        List<Object> objs = output.getResult().get(0).getOutput();
        int notNulls = 0;
        for (Object obj: objs) {
            if (obj != null) {
                notNulls++;
            }
        }
        Assert.assertTrue(notNulls > 5);
    }

    // Test against retired V1.0 matcher -- DerivedColumnsCache
    // Disable the test as SQL Server is shutdown
    @Test(groups = "functional", enabled = false)
    public void testDuns() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "12345" },
                { 123, "chevron.com", 12345 }
        };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[]{ "ID", "Domain", "DUNS" });
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
    }

    @Test(groups = "functional")
    public void testMatchEnrichment() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" }, //
                { 456, "testfakedomain.com", null, null, null, null } };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        input.setPredefinedSelection(null);
        input.setCustomSelection(testMatchInputService.enrichmentSelection());
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
        // Check IsMatched flag & column
        Assert.assertTrue(output.getResult().get(0).isMatched());
        Assert.assertTrue((boolean) (output.getResult().get(0).getOutput().get(2)));
        Assert.assertTrue(output.getResult().get(1).isMatched());
        Assert.assertFalse((boolean) (output.getResult().get(1).getOutput().get(2)));
    }

    @Test(groups = "functional")
    public void testLatticeAccountId() {
        Object[][] data = new Object[][] {{ "chevron.com", "Chevron Corporation" }};
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data,
                new String[] { "Domain", "Name" });
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);

        Integer pos = output.getOutputFields().indexOf("LatticeAccountId");
        Assert.assertEquals(output.getResult().get(0).getOutput().get(pos), "0530001159335");
        log.info(JsonUtils.serialize(output));
    }

    @Test(groups = "functional")
    public void testIsPublicDomain() {
        Object[][] data = new Object[][] { { 123, "my@gmail.com", null, null, null, null } };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);

        Integer pos = output.getOutputFields().indexOf("IsPublicDomain");
        Assert.assertTrue(Boolean.TRUE.equals(output.getResult().get(0).getOutput().get(pos)));
        log.info(JsonUtils.serialize(output));
    }

    /*
     * Match against 1.0.0 DataCloud Version , no fuzzy match involved;
     * Mandatory to add Fake name or fake Duns because need to treat as normal
     * domain and not public domain. Otherwise, lookup by public domain only
     * will be treated as normal domain
     *
     * Disable the test as SQL Server is shutdown
     */
    @Test(groups = "functional", enabled = false)
    public void testExcludePublicDomain() {
        Object[][] data = new Object[][] { { 123, "my@gmail.com", "Fake name", null } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data,
                new String[] { "ID", "Domain", "Name", "Duns" });
        input.setExcludePublicDomain(true);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 1);
        Assert.assertNull(output.getResult().get(0).getOutput());
        Assert.assertFalse(output.getResult().get(0).isMatched());
    }

    /*
     * public domain treated as public domain = when name and DUNS are both null
     * public domain treated as normal domain = when zookeeper relaxPublic
     * domain flag is set and either name or DUNS are provided
     */
    @Test(groups = "functional", dataProvider = "publicDomain")
    public void testRelaxPublicDomainCheck(Integer id, String domain, String name, String duns, boolean isPublicDomain,
            boolean isMatched, String nameKeyword) {
        Object[][] data = new Object[][] {
            { id, domain, name, duns }
        };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data,
                new String[] { "ID", "Domain", "Name", "Duns" });
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("Name"));
        keyMap.put(MatchKey.DUNS, Collections.singletonList("Duns"));
        input.setKeyMap(keyMap);
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = Arrays.asList( //
                new Column(DataCloudConstants.ATTR_LDC_NAME), //
                new Column("IsPublicDomain") //
        );
        columnSelection.setColumns(columns);
        input.setCustomSelection(columnSelection);
        input.setPredefinedSelection(null);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        try {
            camille = CamilleEnvironment.getCamille();
            podId = CamilleEnvironment.getPodId();
            camille.upsert(
                    PathBuilder.buildServicePath(podId, PROPDATA_SERVICE, leStack).append(RELAX_PUBLIC_DOMAIN_CHECK),
                    new Document("true"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            e.printStackTrace();
        }
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().get(0).isMatched(), isMatched);
        if(nameKeyword == null) {
            Assert.assertNull(output.getResult().get(0).getOutput().get(0));
        } else {
            Assert.assertTrue(output.getResult().get(0).getOutput().get(0).toString().contains(nameKeyword));
        }
        Assert.assertEquals(output.getResult().get(0).getOutput().get(1).toString(), String.valueOf(isPublicDomain));
    }

    @DataProvider(name = "publicDomain")
    private Object[][] provideNameLocation() {
        // "ID", "Domain", "Name", "Duns", IsPublicDomain, IsMatched,
        // nameKeyword
        return new Object[][] { //
                // public domain without name and duns : will be
                // treated as normal domain
                { 1, "facebook.com", null, null, true, true, "Facebook" }, //
                // public domain of email format, and without name and duns :
                // will be treated as public domain and no match
                { 2, "xxx@facebook.com", null, null, true, false, null }, //
                // public domain with DUNS (google duns) : will be treated as
                // public domain when matching and match to google entity(as
                // duns provided)
                { 3, "facebook.com", null, "060902413", true, true, "Google" }, //
                // public domain with Valid Name : will be treated as public
                // domain when matching and match to google entity(as name
                // provided)
                { 4, "facebook.com", "Google", null, true, true, "Alphabet" }, //
                // public domain with invalid name : will be treated as public
                // domain and no match
                { 5, "facebook.com", "Fake Name", null, true, false, null }, //
                // non-public domain with name and duns : will match to domain
                // entity
                { 6, "netapp.com", "NetApp", "802054742", false, true, "Netapp" }, //
                // non-public domain with different company name : will match
                // domain entity as valid non-public domain
                { 7, "netapp.com", "Google", null, false, true, "Netapp" }, //
                // non-public domain with duns not in datacloud : will match
                // domain entity as valid non-public domain
                { 8, "netapp.com", null, "999999999", false, true, "Netapp" }, //
                // non-public domain with fake invalid duns : will cleanup duns
                // and match domain entity as valid non-public domain
                { 9, "netapp.com", null, "Fake Duns", false, true, "Netapp" }
        };
    }

    /*
     * Mandatory to add Fake name or Fake Duns because need to treat as normal
     * domain and not public domain. Otherwise, lookup by public domain only
     * will be treated as normal domain
     */
    @Test(groups = "functional")
    public void testPublicDomainEmail() {
        Object[][] data = new Object[][] { { "my@gmail.com", "Fake Name", null } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[]{ "Email", "Name", "Duns" });
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Email"));
        keyMap.put(MatchKey.Name, Collections.singletonList("Name"));
        keyMap.put(MatchKey.DUNS, Collections.singletonList("Duns"));
        input.setKeyMap(keyMap);
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        input.setSkipKeyResolution(true);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 1);
        Assert.assertNotNull(output.getResult().get(0).getOutput());
        Assert.assertFalse(output.getResult().get(0).isMatched());
    }

    /*
     * Mandatory to add Fake name or Fake Duns because need to treat as normal
     * domain and not public domain. Otherwise, lookup by public domain only
     * will be treated as normal domain
     */
    @Test(groups = "functional")
    public void testTwoStepMatching() {
        Object[][] data = new Object[][] {
                { 1, "chevron.com", null, null },
                { 2, "my@gmail.com", "Fake Name", null }
        };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data,
                new String[] { "ID", "Domain", "Name", "Duns" });
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("Name"));
        keyMap.put(MatchKey.DUNS, Collections.singletonList("Duns"));
        input.setKeyMap(keyMap);
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 2);
        Assert.assertEquals(output.getOutputFields().size(), 1);
        Assert.assertEquals(output.getOutputFields().get(0), MatchConstants.LID_FIELD);
        String latticeAccountId = (String) output.getResult().get(0).getOutput().get(0);
        Assert.assertNotNull(latticeAccountId);
        Assert.assertTrue(output.getResult().get(0).isMatched());
        Assert.assertNull(output.getResult().get(1).getOutput().get(0));
        Assert.assertFalse(output.getResult().get(1).isMatched());

        data = new Object[][] {
                { 1, latticeAccountId, "chevron.com", null, null }, //
                { 2, null, "my@gmail.com", "Fake Name", null }
        };
        input = TestMatchInputUtils.prepareSimpleMatchInput(data,
                new String[] { "ID", MatchConstants.LID_FIELD, "Domain", "Name", "Duns" });
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        input.setFetchOnly(true);
        output = realTimeMatchService.match(input);

        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 2);
        Assert.assertEquals(output.getStatistics().getRowsMatched(), new Integer(1));
        output.setMetadata(null);
        log.info("MatchOutput: {}", JsonUtils.serialize(output));
        Assert.assertTrue(output.getResult().get(0).isMatched());
        Assert.assertFalse(output.getResult().get(1).isMatched());
    }

    @Test(groups = "functional")
    public void testStandardizedLatticeIdForInput() {
        final String LATTICE_ID = "0530001159335";
        Object[][] data = new Object[][] {
                { 0, LATTICE_ID },
                { 1, "00000" + LATTICE_ID },
                { 2, "  \t " + LATTICE_ID + " \t " },
                { 3, "00000123456999000" },
                { 4, "12345678901234567890" },
                { 5, "530a0b0cdef11Z59XX33Y5X" },
                { 6, "-" + LATTICE_ID },
                { 7, "+" + LATTICE_ID },
                { 8, "\t 5 300  01159\t33 5   \t  " },
                { 9, "标识符530001标识符159335标识符" },
                { 10, "530a0b0cdef11Z59XX33Y5X" },
                { 11, null },
                { 12, "" },
                { 13, " " },
                { 14, "null" },
                { 15, "Null " },
                { 16, " NULL" },
                { 17, "NULL" },
                { 18, "abcdefgh" },
        };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[] { "ID", "LatticeAccountId" });
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getOutputFields().size(), 1);
        Assert.assertEquals(output.getOutputFields().get(0), MatchConstants.LID_FIELD);
        Assert.assertEquals(output.getStatistics().getColumnMatchCount().size(), 1);
        Assert.assertEquals(output.getStatistics().getColumnMatchCount().get(0), Integer.valueOf(4));
        Assert.assertEquals(output.getResult().size(), data.length);

        for (int i = 0; i < output.getResult().size(); i++) {
            String actualId = output.getResult().get(i).getMatchedLatticeAccountId();
            if (i < 3) {
                Assert.assertEquals(actualId, LATTICE_ID);
            } else if (i == 3) {
                Assert.assertEquals(actualId, "0123456999000");
            } else {
                Assert.assertNull(actualId);
            }
        }
    }

    /**
     * When white cache is expired, assert on expected log could fail. But it
     * will not happen frequently (probably every 3 month). Just retry will
     * pass. Assert on logs is to catch the case that repeatedly fetch duns at
     * both DnBCacheLookup and DnBLookup
     */
    @Test(groups = "functional")
    public void testDunsCorrectness() {
        Map<String, Object[]> provider = testDunsProvider();
        for (Map.Entry<String, Object[]> entry : provider.entrySet()) {
            Object[][] data = new Object[][] { (Object[]) entry.getValue()[1] };
            MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, (String[]) entry.getValue()[0]);
            input.setLogLevelEnum(Level.DEBUG);
            input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
            if (entry.getKey().equals("Key_Loc_Data_Loc_CacheMiss")) {
                removeDnBCacheForCacheMiss((Object[]) entry.getValue()[1]);
            }
            MatchOutput output = realTimeMatchService.match(input);
            log.info("TestCase: " + entry.getKey());
            Assert.assertNotNull(output);
            Assert.assertEquals(output.getResult().size(), 1);
            Assert.assertEquals(output.getStatistics().getRowsMatched(), entry.getValue()[2]);
            Set<String> logs = new HashSet<>();
            for (String log : output.getResult().get(0).getMatchLogs()) {
                logs.add(log.replaceFirst("\\[.+\\] ", ""));
            }
            String[] expectedLogs = (String[]) entry.getValue()[3];
            // Put test case name here to show match log for debug
            if (entry.getKey().equals("")) {
                for (String msg : output.getResult().get(0).getMatchLogs()) {
                    log.info(msg);
                }
            }
            for (String expected : expectedLogs) {
                Assert.assertTrue(logs.contains(expected));
            }
            String[] unexpectedLogs = (String[]) entry.getValue()[4];
            for (String unexpected : unexpectedLogs) {
                Assert.assertFalse(logs.contains(unexpected));
            }
        }
    }

    /*
     * Test the DUNS redirect functionality. Without redirect, we should match to given src DUNS.
     * With redirect, we should get the target DUNS instead.
     */
    @Test(groups = "functional", dataProvider = "provideDunsGuideBookTestData")
    public void testDunsGuideBook(
            MatchKeyTuple tuple, String expectedSrcDuns, String expectedTargetDuns, String expectedBookSource) {
        MatchInput input = TestDunsGuideBookUtils.newRealtimeMatchInput(
                versionEntityMgr.currentApprovedVersionAsString(), DECISION_GRAPH_WITHOUT_GUIDE_BOOK, tuple);
        MatchOutput output = realTimeMatchService.match(input);
        // should get srcDuns using the old decision graph
        verifyMatchedDuns(output, expectedSrcDuns);

        input = TestDunsGuideBookUtils.newRealtimeMatchInput(
                versionEntityMgr.currentApprovedVersionAsString(), DECISION_GRAPH_WITH_GUIDE_BOOK, tuple);
        output = realTimeMatchService.match(input);
        // redirect to the target DUNS
        verifyMatchedDuns(output, expectedTargetDuns);
        if (expectedBookSource != null) {
            // should be redirected with the correct keyPartition & bookSource
            Assert.assertTrue(containsRedirectLog(output, tuple, expectedBookSource));
        }
    }

    /**
     * All cases covered: Key_Loc_Data_Loc_CacheAccept
     * Key_Loc_Data_Loc_CacheDiscard Key_Loc_Data_Loc_CacheMiss
     * Key_Duns_Data_DunsMatch Key_Duns_Data_DunsUnmatch Key_Duns_Data_DunsNull
     * Key_Duns_Data_DunsNullString Key_Duns_Data_DunsInvalid
     * Key_DunsLoc_Data_Loc_CacheAccept Key_DunsLoc_Data_Loc_CacheDiscard
     * Key_DunsLoc_Data_Duns Key_DunsLoc_Data_DunsLoc_DunsMatch
     * Key_DunsLoc_Data_DunsLoc_DunsUnatchLocCacheHitAccept
     * Key_DunsLoc_Data_DunsLoc_DunsUnatchLocCacheHitDiscard
     */
    private Map<String, Object[]> testDunsProvider() {
        Map<String, Object[]> map = new HashMap<>();
        map.put("Key_Loc_Data_Loc_CacheAccept", //
                testDunsExpectedResuls(new String[] { "Name", "State" }, //
                        new Object[] { "Google", "CA" }, 1, new String[] { "Rejected by DunsBasedMicroEngineActor", //
                                "Retrieved a DUNS from white cache using Id=_CITY_NULL_COUNTRYCODE_US_NAME_GOOGLE_PHONE_NULL_STATE_CALIFORNIA_ZIPCODE_NULL. Did not go to remote DnB API.", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                        }, new String[] {}));
        map.put("Key_Loc_Data_Loc_CacheDiscard", //
                testDunsExpectedResuls(new String[] { "Name", "State" }, //
                        new Object[] { "Google123456", "CA" }, 0,
                        new String[] { "Rejected by DunsBasedMicroEngineActor", //
                                "Encountered an issue with DUNS lookup at LocationToCachedDunsMicroEngineActor: Match result does not meet acceptance criteria, discarded.", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                                "Skipping DunsBasedMicroEngineActor because this is the second visit with the same context.",//
                        }, new String[] {}));
        map.put("Key_Loc_Data_Loc_CacheMiss", //
                testDunsExpectedResuls(new String[] { "Name", "Country" }, //
                        new Object[] { "LONGSHINE", "CHINA" }, 1,
                        new String[] { "Rejected by DunsBasedMicroEngineActor", //
                                "Did not hit either white or black cache.", //
                                "Went to remote DnB API.", //
                        }, new String[] { "Rejected by LocationToDunsMicroEngineActor", //
                        }));
        map.put("Key_Duns_Data_DunsMatch", //
                testDunsExpectedResuls(new String[] { "DUNS" }, //
                        new Object[] { " 060902413  " }, 1, new String[] {},
                        new String[] { "Arrived LocationToCachedDunsMicroEngineActor.", //
                                "Arrived LocationToDunsMicroEngineActor.",//
                        }));
        map.put("Key_Duns_Data_DunsUnmatch", //
                testDunsExpectedResuls(new String[] { "DUNS" }, //
                        new Object[] { "123" }, 0,
                        new String[] { "Did not get any luck at DunsBasedMicroEngineActor with ( DUNS=000000123 )", //
                                "Rejected by LocationToCachedDunsMicroEngineActor", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                                "Skipping DunsBasedMicroEngineActor because this is the second visit with the same context." },
                        new String[] {}));
        map.put("Key_Duns_Data_DunsNull", //
                testDunsExpectedResuls(new String[] { "DUNS" }, //
                        new Object[] { null }, 0, new String[] { "Has ( ) to begin with.", //
                                "Rejected by DunsBasedMicroEngineActor", //
                                "Rejected by LocationToCachedDunsMicroEngineActor", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                                "Skipping DunsBasedMicroEngineActor because this is the second visit with the same context.",//
                        }, new String[] {}));
        map.put("Key_Duns_Data_DunsNullString", //
                testDunsExpectedResuls(new String[] { "DUNS" }, //
                        new Object[] { "null" }, 0, new String[] { "Has ( ) to begin with.", //
                                "Rejected by DunsBasedMicroEngineActor", //
                                "Rejected by LocationToCachedDunsMicroEngineActor", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                                "Skipping DunsBasedMicroEngineActor because this is the second visit with the same context.",//
                        }, new String[] {}));
        map.put("Key_Duns_Data_DunsInvalid", //
                testDunsExpectedResuls(new String[] { "DUNS" }, //
                        new Object[] { "0123456789" }, 0, new String[] { "Has ( ) to begin with.", //
                                "Rejected by DunsBasedMicroEngineActor", //
                                "Rejected by LocationToCachedDunsMicroEngineActor", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                                "Skipping DunsBasedMicroEngineActor because this is the second visit with the same context.",//
                        }, new String[] {}));
        map.put("Key_DunsLoc_Data_Loc_CacheAccept", //
                testDunsExpectedResuls(new String[] { "DUNS", "Name", "State" }, //
                        new Object[] { null, "Google", "CA" }, 1,
                        new String[] { "Rejected by DunsBasedMicroEngineActor", //
                                "Retrieved a DUNS from white cache using Id=_CITY_NULL_COUNTRYCODE_US_NAME_GOOGLE_PHONE_NULL_STATE_CALIFORNIA_ZIPCODE_NULL. Did not go to remote DnB API.", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                        }, new String[] {}));
        map.put("Key_DunsLoc_Data_Loc_CacheDiscard", //
                testDunsExpectedResuls(new String[] { "DUNS", "Name", "State" }, //
                        new Object[] { null, "Google123456", "CA" }, 0,
                        new String[] { "Rejected by DunsBasedMicroEngineActor", //
                                "Encountered an issue with DUNS lookup at LocationToCachedDunsMicroEngineActor: Match result does not meet acceptance criteria, discarded.", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                                "Skipping DunsBasedMicroEngineActor because this is the second visit with the same context.",//
                        }, new String[] {}));
        map.put("Key_DunsLoc_Data_Duns", //
                testDunsExpectedResuls(new String[] { "DUNS", "Name", "State" }, //
                        new Object[] { " 060902413  ", null, null }, 1, new String[] {},
                        new String[] { "Arrived LocationToCachedDunsMicroEngineActor.", //
                                "Arrived LocationToDunsMicroEngineActor.",//
                        }));
        map.put("Key_DunsLoc_Data_DunsLoc_DunsMatch", //
                testDunsExpectedResuls(new String[] { "DUNS", "Name", "State" }, //
                        new Object[] { " 060902413  ", "Google", "CA" }, 1, new String[] {},
                        new String[] { "Arrived LocationToCachedDunsMicroEngineActor.", //
                                "Arrived LocationToDunsMicroEngineActor.",//
                        }));
        map.put("Key_DunsLoc_Data_DunsLoc_DunsUnatchLocCacheHitAccept", //
                testDunsExpectedResuls(new String[] { "DUNS", "Name", "State" }, //
                        new Object[] { "123", "Google", "CA" }, 1,
                        new String[] { "Did not get any luck at DunsBasedMicroEngineActor with ( DUNS=000000123 )", //
                                "Retrieved a DUNS from white cache using Id=_CITY_NULL_COUNTRYCODE_US_NAME_GOOGLE_PHONE_NULL_STATE_CALIFORNIA_ZIPCODE_NULL. Did not go to remote DnB API.", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                        }, new String[] {}));
        map.put("Key_DunsLoc_Data_DunsLoc_DunsUnatchLocCacheHitDiscard", //
                testDunsExpectedResuls(new String[] { "DUNS", "Name", "State" }, //
                        new Object[] { "123", "Google123456", "CA" }, 0, new String[] { //
                                "Did not get any luck at DunsBasedMicroEngineActor with ( DUNS=000000123 )", //
                                "Retrieved a DUNS from white cache using Id=_CITY_NULL_COUNTRYCODE_US_NAME_GOOGLE123456_PHONE_NULL_STATE_CALIFORNIA_ZIPCODE_NULL. Did not go to remote DnB API.", //
                                "Encountered an issue with DUNS lookup at LocationToCachedDunsMicroEngineActor: Match result does not meet acceptance criteria, discarded.", //
                                "Rejected by LocationToDunsMicroEngineActor", //
                                "Skipping DunsBasedMicroEngineActor because this is the second visit with the same context.", //
                        }, new String[] {}));
        return map;
    }

    @DataProvider(name = "provideDunsGuideBookTestData")
    private Object[][] provideDunsGuideBookTestData() {
        return TestDunsGuideBookUtils.getDunsGuideBookTestData();
    }

    private Object[] testDunsExpectedResuls(String[] matchKeys, Object[] data, int matchedRows, String[] expectedLogs,
            String[] unexpectedLogs) {
        return new Object[] { matchKeys, data, matchedRows, expectedLogs, unexpectedLogs };
    }

    private void removeDnBCacheForCacheMiss(Object[] data) {
        NameLocation nl = new NameLocation();
        nl.setName((String) data[0]);
        nl.setCountryCode(countryCodeService.getCountryCode((String) data[1]));
        dnbCacheService.removeCache(new DnBCache(nl));
    }

    private void verifyMatchedDuns(MatchOutput output, String matchedDuns) {
        Assert.assertNotNull(output);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), 1);
        OutputRecord record = output.getResult().get(0);
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getMatchedDuns(), matchedDuns);
    }

    /*
     * Use match logs to determine whether redirection happens with the correct key partition and book source
     */
    private boolean containsRedirectLog(
            @NotNull MatchOutput output, @NotNull MatchKeyTuple tuple, @NotNull String bookSource) {
        tuple.setCountryCode(tuple.getCountry());
        String keyPartitionLog = String.format("KeyPartition=%s", MatchKeyUtils.evalKeyPartition(tuple));
        String bookSourceLog = String.format("BookSource=%s", bookSource);
        OutputRecord record = output.getResult().get(0);
        Assert.assertNotNull(record.getMatchLogs());
        for (String log : record.getMatchLogs()) {
            if (log != null && log.contains(keyPartitionLog) && log.contains(bookSourceLog)) {
                return true;
            }
        }
        return false;
    }
}
