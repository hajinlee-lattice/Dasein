package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.datacloud.match.testframework.TestMatchInputUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component
public class RealTimeMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RealTimeMatchServiceImplTestNG.class);

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

    @Test(groups = "functional")
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

    @Test(groups = "functional")
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

        MatchOutput output = bulkMatchOutput.getOutputList().get(0);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), 3);
    }

    @Test(groups = "functional")
    public void testSimpleMatchAccountMaster() {
        Object[][] data = new Object[][] {
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);

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

    @Test(groups = "functional")
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
                { 123, "chevron.com", "Chevron Corporation", "San Ramon", "California", "USA" } };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        input.setPredefinedSelection(null);
        input.setCustomSelection(testMatchInputService.enrichmentSelection());
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertTrue(output.getResult().size() > 0);
        Assert.assertTrue(output.getStatistics().getRowsMatched() > 0);
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
    }

    @Test(groups = "functional")
    public void testExcludePublicDomain() {
        Object[][] data = new Object[][] { { 123, "my@gmail.com", null, null, null, null } };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data);
        input.setExcludePublicDomain(true);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 1);
        Assert.assertNull(output.getResult().get(0).getOutput());
        Assert.assertFalse(output.getResult().get(0).isMatched());
    }

    @Test(groups = "functional")
    public void testPublicDomainEmail() {
        Object[][] data = new Object[][] { { "my@gmail.com" } };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[]{ "Email" });
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Email"));
        input.setKeyMap(keyMap);
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setDataCloudVersion("1.0.0");
        input.setSkipKeyResolution(true);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 1);
        Assert.assertNotNull(output.getResult().get(0).getOutput());
        Assert.assertFalse(output.getResult().get(0).isMatched());
    }

    @Test(groups = "functional")
    public void testTwoStepMatching() {
        Object[][] data = new Object[][] {
                { 1, "chevron.com" },
                { 2, "my@gmail.com" }
        };
        MatchInput input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[] { "ID", "Domain" });
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 2);
        Assert.assertEquals(output.getOutputFields().size(), 1);
        Assert.assertEquals(output.getOutputFields().get(0), MatchConstants.LID_FIELD);

        String latticeAccountId = (String) output.getResult().get(0).getOutput().get(0);
        Assert.assertNotNull(latticeAccountId);
        Assert.assertNull(output.getResult().get(1).getOutput().get(0));

        data = new Object[][] {
                { 1, latticeAccountId, "chevron.com" },
                { 2, null, "my@gmail.com" }
        };
        input = TestMatchInputUtils.prepareSimpleMatchInput(data, new String[] { "ID", MatchConstants.LID_FIELD, "Domain" });
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersion().getVersion());
        input.setFetchOnly(true);
        output = realTimeMatchService.match(input);

        Assert.assertNotNull(output);
        Assert.assertEquals(output.getResult().size(), 2);
        Assert.assertEquals(output.getStatistics().getRowsMatched(), new Integer(1));
        output.setMetadata(null);
        System.out.println(JsonUtils.serialize(output));
    }

    @Test(groups = "functional")
    public void testStandardizedLatticeIdForInput() {
        final String LATTICE_ID = "530001159335";
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
                Assert.assertEquals(actualId, "123456999000");
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
            input.setLogLevel(Level.DEBUG);
            input.setDataCloudVersion("2.0.7");
            if (entry.getKey().equals("Key_Loc_Data_Loc_CacheMiss")) {
                removeDnBCacheForCacheMiss((Object[]) entry.getValue()[1]);
            }
            MatchOutput output = realTimeMatchService.match(input);
            log.info("TestCase: " + entry.getKey());
            Assert.assertNotNull(output);
            Assert.assertEquals(output.getResult().size(), 1);
            Assert.assertEquals(output.getStatistics().getRowsMatched(), (Integer) entry.getValue()[2]);
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
}
