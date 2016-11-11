package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBlackCache;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DnBCacheServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(DnBCacheServiceImplTestNG.class);

    public static final String VERSION = "2.0.0";

    @Autowired
    private DnBCacheService dnbCacheService;

    private List<DnBWhiteCache> whiteCaches = new ArrayList<DnBWhiteCache>();

    private List<DnBBlackCache> blackCaches = new ArrayList<DnBBlackCache>();

    @AfterClass(groups = "functional", enabled = false)
    public void afterClass() {
        deleteEntities();
    }

    private void deleteEntities() {
        Assert.assertTrue(!whiteCaches.isEmpty());
        for (DnBWhiteCache whiteCache : whiteCaches) {
            dnbCacheService.getWhiteCacheMgr(VERSION).delete(whiteCache);
        }
        Assert.assertTrue(!blackCaches.isEmpty());
        for (DnBBlackCache blackCache : blackCaches) {
            dnbCacheService.getBlackCacheMgr(VERSION).delete(blackCache);
        }
    }
    
    /*********************************
     * White Cache
     *********************************/

    @Test(groups = "functional", enabled = false)
    public void testCreateWhiteCache() {
        Object[][] data = getEntityInputData();
        DnBMatchContext context = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName((String) data[0][0]);
        nameLocation.setCountryCode((String) data[0][1]);
        nameLocation.setState((String) data[0][2]);
        nameLocation.setCity((String) data[0][3]);
        nameLocation.setPhoneNumber((String) data[0][4]);
        nameLocation.setZipcode((String) data[0][5]);
        context.setInputNameLocation(nameLocation);
        context.setInputEmail((String) data[0][6]);
        context.setDuns((String) data[0][7]);
        context.setConfidenceCode((Integer) data[0][8]);
        context.setMatchGrade((String) data[0][9]);
        whiteCaches.add(dnbCacheService.addWhiteCache(context, VERSION));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateWhiteCache", enabled = false)
    public void testBatchCreateWhiteCache() {
        Object[][] data = getEntityInputData();
        List<DnBMatchContext> contexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < data.length - 1; i++) {
            DnBMatchContext context = new DnBMatchContext();
            NameLocation nameLocation = new NameLocation();
            nameLocation.setName((String) data[i][0]);
            nameLocation.setCountryCode((String) data[i][1]);
            nameLocation.setState((String) data[i][2]);
            nameLocation.setCity((String) data[i][3]);
            nameLocation.setPhoneNumber((String) data[i][4]);
            nameLocation.setZipcode((String) data[i][5]);
            context.setInputNameLocation(nameLocation);
            context.setInputEmail((String) data[i][6]);
            context.setDuns((String) data[i][7]);
            context.setConfidenceCode((Integer) data[i][8]);
            context.setMatchGrade((String) data[i][9]);
            contexts.add(context);
        }
        whiteCaches.addAll(dnbCacheService.batchAddWhiteCache(contexts, VERSION));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWhiteCache",
            "testBatchCreateWhiteCache" }, enabled = false)
    public void testLookupWhiteCache() {
        MatchKeyTuple hit = new MatchKeyTuple();
        Object[][] data = getEntityInputData();
        hit.setName((String) data[0][0]);
        hit.setCountryCode((String) data[0][1]);
        hit.setState((String) data[0][2]);
        hit.setCity((String) data[0][3]);
        hit.setPhoneNumber((String) data[0][4]);
        hit.setZipcode((String) data[0][5]);
        hit.setEmail((String) data[0][6]);
        DnBWhiteCache whiteCache = dnbCacheService.lookupWhiteCache(hit, VERSION);
        Assert.assertEquals(whiteCache.getDuns(), (String) data[0][7]);
        Assert.assertEquals(whiteCache.getConfidenceCode(), (Integer) data[0][8]);
        Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), (String) data[0][9]);
        MatchKeyTuple miss = new MatchKeyTuple();
        miss.setName((String) data[data.length - 1][0]);
        miss.setCountryCode((String) data[data.length - 1][1]);
        miss.setState((String) data[data.length - 1][2]);
        miss.setCity((String) data[data.length - 1][3]);
        miss.setPhoneNumber((String) data[data.length - 1][4]);
        miss.setZipcode((String) data[data.length - 1][5]);
        miss.setEmail((String) data[data.length - 1][6]);
        Assert.assertNull(dnbCacheService.lookupWhiteCache(miss, VERSION));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWhiteCache",
            "testBatchCreateWhiteCache" }, enabled = false)
    public void testBatchLookupWhiteCache() {
        Object[][] data = getEntityInputData();
        Map<String, MatchKeyTuple> matchKeyTuples = new HashMap<String, MatchKeyTuple>();
        for (int i = 1; i < data.length; i++) {
            MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
            matchKeyTuple.setName((String) data[i][0]);
            matchKeyTuple.setCountryCode((String) data[i][1]);
            matchKeyTuple.setState((String) data[i][2]);
            matchKeyTuple.setCity((String) data[i][3]);
            matchKeyTuple.setPhoneNumber((String) data[i][4]);
            matchKeyTuple.setZipcode((String) data[i][5]);
            matchKeyTuple.setEmail((String) data[i][6]);
            matchKeyTuples.put(String.valueOf(i), matchKeyTuple);
        }
        Map<String, DnBWhiteCache> whiteCaches = dnbCacheService.batchLookupWhiteCache(matchKeyTuples, VERSION);
        Assert.assertEquals(whiteCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBWhiteCache whiteCache = whiteCaches.get(String.valueOf(i));
            Assert.assertNotNull(whiteCache);
            Assert.assertEquals(whiteCache.getDuns(), (String) data[i][7]);
            Assert.assertEquals(whiteCache.getConfidenceCode(), (Integer) data[i][8]);
            Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), (String) data[i][9]);
        }
        Assert.assertTrue(!whiteCaches.containsKey(String.valueOf(data.length - 1)));
    }

    /*********************************
     * Black Cache
     *********************************/

    @Test(groups = "functional", enabled = false)
    public void testCreateBlackCache() {
        Object[][] data = getEntityInputData();
        DnBMatchContext context = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName((String) data[0][0]);
        nameLocation.setCountryCode((String) data[0][1]);
        nameLocation.setState((String) data[0][2]);
        nameLocation.setCity((String) data[0][3]);
        nameLocation.setPhoneNumber((String) data[0][4]);
        nameLocation.setZipcode((String) data[0][5]);
        context.setInputNameLocation(nameLocation);
        context.setInputEmail((String) data[0][6]);
        blackCaches.add(dnbCacheService.addBlackCache(context, VERSION));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateBlackCache", enabled = false)
    public void testBatchCreateBlackCache() {
        Object[][] data = getEntityInputData();
        List<DnBMatchContext> contexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < data.length - 1; i++) {
            DnBMatchContext context = new DnBMatchContext();
            NameLocation nameLocation = new NameLocation();
            nameLocation.setName((String) data[i][0]);
            nameLocation.setCountryCode((String) data[i][1]);
            nameLocation.setState((String) data[i][2]);
            nameLocation.setCity((String) data[i][3]);
            nameLocation.setPhoneNumber((String) data[i][4]);
            nameLocation.setZipcode((String) data[i][5]);
            context.setInputNameLocation(nameLocation);
            context.setInputEmail((String) data[i][6]);
            contexts.add(context);
        }
        blackCaches.addAll(dnbCacheService.batchAddBlackCache(contexts, VERSION));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateBlackCache",
            "testBatchCreateBlackCache" }, enabled = false)
    public void testLookupBlackCache() {
        MatchKeyTuple hit = new MatchKeyTuple();
        Object[][] data = getEntityInputData();
        hit.setName((String) data[0][0]);
        hit.setCountryCode((String) data[0][1]);
        hit.setState((String) data[0][2]);
        hit.setCity((String) data[0][3]);
        hit.setPhoneNumber((String) data[0][4]);
        hit.setZipcode((String) data[0][5]);
        hit.setEmail((String) data[0][6]);
        DnBBlackCache blackCache = dnbCacheService.lookupBlackCache(hit, VERSION);
        Assert.assertNotNull(blackCache);
        Assert.assertNotNull(blackCache.getId());
        MatchKeyTuple miss = new MatchKeyTuple();
        miss.setName((String) data[data.length - 1][0]);
        miss.setCountryCode((String) data[data.length - 1][1]);
        miss.setState((String) data[data.length - 1][2]);
        miss.setCity((String) data[data.length - 1][3]);
        miss.setPhoneNumber((String) data[data.length - 1][4]);
        miss.setZipcode((String) data[data.length - 1][5]);
        miss.setEmail((String) data[data.length - 1][6]);
        Assert.assertNull(dnbCacheService.lookupBlackCache(miss, VERSION));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateBlackCache",
            "testBatchCreateBlackCache" }, enabled = false)
    public void testBatchLookupBlackCache() {
        Object[][] data = getEntityInputData();
        Map<String, MatchKeyTuple> matchKeyTuples = new HashMap<String, MatchKeyTuple>();
        for (int i = 1; i < data.length; i++) {
            MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
            matchKeyTuple.setName((String) data[i][0]);
            matchKeyTuple.setCountryCode((String) data[i][1]);
            matchKeyTuple.setState((String) data[i][2]);
            matchKeyTuple.setCity((String) data[i][3]);
            matchKeyTuple.setPhoneNumber((String) data[i][4]);
            matchKeyTuple.setZipcode((String) data[i][5]);
            matchKeyTuple.setEmail((String) data[i][6]);
            matchKeyTuples.put(String.valueOf(i), matchKeyTuple);
        }
        Map<String, DnBBlackCache> blackCaches = dnbCacheService.batchLookupBlackCache(matchKeyTuples, VERSION);
        Assert.assertEquals(blackCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBBlackCache blackCache = blackCaches.get(String.valueOf(i));
            Assert.assertNotNull(blackCache);
            Assert.assertNotNull(blackCache.getId());
        }
        Assert.assertTrue(!blackCaches.containsKey(String.valueOf(data.length - 1)));
    }

    private static Object[][] getEntityInputData() {
        return new Object[][] {
                { "DUMMY_NAME1", "DUMMY_COUNTRYCODE1", "DUMMY_STATE1", "DUMMY_CITY1", "DUMMY_PHONE1",
                        "DUMMY_ZIPCODE1", "DUMMY_EMAIL1", "DUMMY_DUNS1", 7, "DUMMY_MATCHGRADE1" },
                { "DUMMY_NAME2", "DUMMY_COUNTRYCODE2", "DUMMY_STATE2", "DUMMY_CITY2", "DUMMY_PHONE2",
                        "DUMMY_ZIPCODE2", "DUMMY_EMAIL2", "DUMMY_DUNS2", 5, "DUMMY_MATCHGRADE2" },
                { "DUMMY_NAME3", "DUMMY_COUNTRYCODE3", "DUMMY_STATE3", "DUMMY_CITY3", "DUMMY_PHONE3", "DUMMY_ZIPCODE3",
                        "DUMMY_EMAIL3", "DUMMY_DUNS3", 6, "DUMMY_MATCHGRADE3" },
                { "DUMMY_NAME4", "DUMMY_COUNTRYCODE4", "DUMMY_STATE4", "DUMMY_CITY4", "DUMMY_PHONE4", "DUMMY_ZIPCODE4",
                        "DUMMY_EMAIL4", "DUMMY_DUNS4", 6, "DUMMY_MATCHGRADE4" } };
    }
}
