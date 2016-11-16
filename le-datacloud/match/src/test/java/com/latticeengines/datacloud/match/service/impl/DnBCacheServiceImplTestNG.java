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

import com.latticeengines.datacloud.match.dnb.DnBBlackCache;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DnBCacheServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(DnBCacheServiceImplTestNG.class);

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
            dnbCacheService.getWhiteCacheMgr().delete(whiteCache);
        }
        Assert.assertTrue(!blackCaches.isEmpty());
        for (DnBBlackCache blackCache : blackCaches) {
            dnbCacheService.getBlackCacheMgr().delete(blackCache);
        }
    }
    
    /*********************************
     * White Cache
     *********************************/

    @Test(groups = "functional", enabled = false)
    public void testCreateWhiteCache() {
        Object[][] data = getEntityInputData();
        DnBMatchContext entityContext = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName((String) data[0][0]);
        nameLocation.setCountryCode((String) data[0][1]);
        nameLocation.setState((String) data[0][2]);
        nameLocation.setCity((String) data[0][3]);
        nameLocation.setPhoneNumber((String) data[0][4]);
        nameLocation.setZipcode((String) data[0][5]);
        entityContext.setInputNameLocation(nameLocation);
        entityContext.setDuns((String) data[0][7]);
        entityContext.setConfidenceCode((Integer) data[0][8]);
        entityContext.setMatchGrade((String) data[0][9]);
        entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        whiteCaches.add(dnbCacheService.addWhiteCache(entityContext));
        
        DnBMatchContext emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[0][6]);
        emailContext.setDuns((String) data[0][7]);
        emailContext.setConfidenceCode((Integer) data[0][8]);
        emailContext.setMatchGrade((String) data[0][9]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        whiteCaches.add(dnbCacheService.addWhiteCache(emailContext));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateWhiteCache", enabled = false)
    public void testBatchCreateWhiteCache() {
        Object[][] data = getEntityInputData();
        List<DnBMatchContext> entityContexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < data.length - 1; i++) {
            DnBMatchContext entityContext = new DnBMatchContext();
            NameLocation nameLocation = new NameLocation();
            nameLocation.setName((String) data[i][0]);
            nameLocation.setCountryCode((String) data[i][1]);
            nameLocation.setState((String) data[i][2]);
            nameLocation.setCity((String) data[i][3]);
            nameLocation.setPhoneNumber((String) data[i][4]);
            nameLocation.setZipcode((String) data[i][5]);
            entityContext.setInputNameLocation(nameLocation);
            entityContext.setDuns((String) data[i][7]);
            entityContext.setConfidenceCode((Integer) data[i][8]);
            entityContext.setMatchGrade((String) data[i][9]);
            entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
            entityContexts.add(entityContext);
        }
        whiteCaches.addAll(dnbCacheService.batchAddWhiteCache(entityContexts));

        List<DnBMatchContext> emailContexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < data.length - 1; i++) {
            DnBMatchContext emailContext = new DnBMatchContext();
            emailContext.setInputEmail((String) data[i][6]);
            emailContext.setDuns((String) data[i][7]);
            emailContext.setConfidenceCode((Integer) data[i][8]);
            emailContext.setMatchGrade((String) data[i][9]);
            emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
            emailContexts.add(emailContext);
        }
        whiteCaches.addAll(dnbCacheService.batchAddWhiteCache(emailContexts));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWhiteCache",
            "testBatchCreateWhiteCache" }, enabled = false)
    public void testLookupWhiteCache() {
        Object[][] data = getEntityInputData();
        // cache hit
        DnBMatchContext entityContext = new DnBMatchContext();
        entityContext.getInputNameLocation().setName((String) data[0][0]);
        entityContext.getInputNameLocation().setCountryCode((String) data[0][1]);
        entityContext.getInputNameLocation().setState((String) data[0][2]);
        entityContext.getInputNameLocation().setCity((String) data[0][3]);
        entityContext.getInputNameLocation().setPhoneNumber((String) data[0][4]);
        entityContext.getInputNameLocation().setZipcode((String) data[0][5]);
        entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBWhiteCache whiteCache = dnbCacheService.lookupWhiteCache(entityContext);
        Assert.assertEquals(whiteCache.getDuns(), (String) data[0][7]);
        Assert.assertEquals(whiteCache.getConfidenceCode(), (Integer) data[0][8]);
        Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), (String) data[0][9]);

        DnBMatchContext emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[0][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        whiteCache = dnbCacheService.lookupWhiteCache(emailContext);
        Assert.assertEquals(whiteCache.getDuns(), (String) data[0][7]);
        Assert.assertEquals(whiteCache.getConfidenceCode(), (Integer) data[0][8]);
        Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), (String) data[0][9]);

        // cache miss
        entityContext = new DnBMatchContext();
        entityContext.getInputNameLocation().setName((String) data[data.length - 1][0]);
        entityContext.getInputNameLocation().setCountryCode((String) data[data.length - 1][1]);
        entityContext.getInputNameLocation().setState((String) data[data.length - 1][2]);
        entityContext.getInputNameLocation().setCity((String) data[data.length - 1][3]);
        entityContext.getInputNameLocation().setPhoneNumber((String) data[data.length - 1][4]);
        entityContext.getInputNameLocation().setZipcode((String) data[data.length - 1][5]);
        entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        Assert.assertNull(dnbCacheService.lookupWhiteCache(entityContext));

        emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[data.length - 1][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        Assert.assertNull(dnbCacheService.lookupWhiteCache(emailContext));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWhiteCache",
            "testBatchCreateWhiteCache" }, enabled = false)
    public void testBatchLookupWhiteCache() {
        Object[][] data = getEntityInputData();
        Map<String, DnBMatchContext> entityContexts = new HashMap<String, DnBMatchContext>();
        for (int i = 1; i < data.length; i++) {
            DnBMatchContext entityContext = new DnBMatchContext();
            entityContext.getInputNameLocation().setName((String) data[i][0]);
            entityContext.getInputNameLocation().setCountryCode((String) data[i][1]);
            entityContext.getInputNameLocation().setState((String) data[i][2]);
            entityContext.getInputNameLocation().setCity((String) data[i][3]);
            entityContext.getInputNameLocation().setPhoneNumber((String) data[i][4]);
            entityContext.getInputNameLocation().setZipcode((String) data[i][5]);
            entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
            entityContexts.put(String.valueOf(i), entityContext);
        }
        Map<String, DnBWhiteCache> whiteCaches = dnbCacheService.batchLookupWhiteCache(entityContexts);
        Assert.assertEquals(whiteCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBWhiteCache whiteCache = whiteCaches.get(String.valueOf(i));
            Assert.assertNotNull(whiteCache);
            Assert.assertEquals(whiteCache.getDuns(), (String) data[i][7]);
            Assert.assertEquals(whiteCache.getConfidenceCode(), (Integer) data[i][8]);
            Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), (String) data[i][9]);
        }
        Assert.assertTrue(!whiteCaches.containsKey(String.valueOf(data.length - 1)));

        Map<String, DnBMatchContext> emailContexts = new HashMap<String, DnBMatchContext>();
        for (int i = 1; i < data.length; i++) {
            DnBMatchContext emailContext = new DnBMatchContext();
            emailContext.setInputEmail((String) data[i][6]);
            emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
            emailContexts.put(String.valueOf(i), emailContext);
        }
        whiteCaches = dnbCacheService.batchLookupWhiteCache(emailContexts);
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
        DnBMatchContext entityContext = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName((String) data[0][0]);
        nameLocation.setCountryCode((String) data[0][1]);
        nameLocation.setState((String) data[0][2]);
        nameLocation.setCity((String) data[0][3]);
        nameLocation.setPhoneNumber((String) data[0][4]);
        nameLocation.setZipcode((String) data[0][5]);
        entityContext.setInputNameLocation(nameLocation);
        entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        blackCaches.add(dnbCacheService.addBlackCache(entityContext));

        DnBMatchContext emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[0][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        blackCaches.add(dnbCacheService.addBlackCache(emailContext));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateBlackCache", enabled = false)
    public void testBatchCreateBlackCache() {
        Object[][] data = getEntityInputData();
        List<DnBMatchContext> entityContexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < data.length - 1; i++) {
            DnBMatchContext entityContext = new DnBMatchContext();
            NameLocation nameLocation = new NameLocation();
            nameLocation.setName((String) data[i][0]);
            nameLocation.setCountryCode((String) data[i][1]);
            nameLocation.setState((String) data[i][2]);
            nameLocation.setCity((String) data[i][3]);
            nameLocation.setPhoneNumber((String) data[i][4]);
            nameLocation.setZipcode((String) data[i][5]);
            entityContext.setInputNameLocation(nameLocation);
            entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
            entityContexts.add(entityContext);
        }
        blackCaches.addAll(dnbCacheService.batchAddBlackCache(entityContexts));

        List<DnBMatchContext> emailContexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < data.length - 1; i++) {
            DnBMatchContext emailContext = new DnBMatchContext();
            emailContext.setInputEmail((String) data[i][6]);
            emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
            emailContexts.add(emailContext);
        }
        blackCaches.addAll(dnbCacheService.batchAddBlackCache(emailContexts));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateBlackCache",
            "testBatchCreateBlackCache" }, enabled = false)
    public void testLookupBlackCache() {
        Object[][] data = getEntityInputData();
        // cache hit
        DnBMatchContext entityContext = new DnBMatchContext();
        entityContext.getInputNameLocation().setName((String) data[0][0]);
        entityContext.getInputNameLocation().setCountryCode((String) data[0][1]);
        entityContext.getInputNameLocation().setState((String) data[0][2]);
        entityContext.getInputNameLocation().setCity((String) data[0][3]);
        entityContext.getInputNameLocation().setPhoneNumber((String) data[0][4]);
        entityContext.getInputNameLocation().setZipcode((String) data[0][5]);
        entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBBlackCache blackCache = dnbCacheService.lookupBlackCache(entityContext);
        Assert.assertNotNull(blackCache);

        DnBMatchContext emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[0][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        blackCache = dnbCacheService.lookupBlackCache(emailContext);
        Assert.assertNotNull(blackCache);

        // cache miss
        entityContext = new DnBMatchContext();
        entityContext.getInputNameLocation().setName((String) data[data.length - 1][0]);
        entityContext.getInputNameLocation().setCountryCode((String) data[data.length - 1][1]);
        entityContext.getInputNameLocation().setState((String) data[data.length - 1][2]);
        entityContext.getInputNameLocation().setCity((String) data[data.length - 1][3]);
        entityContext.getInputNameLocation().setPhoneNumber((String) data[data.length - 1][4]);
        entityContext.getInputNameLocation().setZipcode((String) data[data.length - 1][5]);
        entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        Assert.assertNull(dnbCacheService.lookupBlackCache(entityContext));

        emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[data.length - 1][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        Assert.assertNull(dnbCacheService.lookupBlackCache(emailContext));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateBlackCache",
            "testBatchCreateBlackCache" }, enabled = false)
    public void testBatchLookupBlackCache() {
        Object[][] data = getEntityInputData();
        Map<String, DnBMatchContext> entityContexts = new HashMap<String, DnBMatchContext>();
        for (int i = 1; i < data.length; i++) {
            DnBMatchContext entityContext = new DnBMatchContext();
            entityContext.getInputNameLocation().setName((String) data[i][0]);
            entityContext.getInputNameLocation().setCountryCode((String) data[i][1]);
            entityContext.getInputNameLocation().setState((String) data[i][2]);
            entityContext.getInputNameLocation().setCity((String) data[i][3]);
            entityContext.getInputNameLocation().setPhoneNumber((String) data[i][4]);
            entityContext.getInputNameLocation().setZipcode((String) data[i][5]);
            entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
            entityContexts.put(String.valueOf(i), entityContext);
        }
        Map<String, DnBBlackCache> blackCaches = dnbCacheService.batchLookupBlackCache(entityContexts);
        Assert.assertEquals(blackCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBBlackCache blackCache = blackCaches.get(String.valueOf(i));
            Assert.assertNotNull(blackCache);
        }
        Assert.assertTrue(!blackCaches.containsKey(String.valueOf(data.length - 1)));

        Map<String, DnBMatchContext> emailContexts = new HashMap<String, DnBMatchContext>();
        for (int i = 1; i < data.length; i++) {
            DnBMatchContext emailContext = new DnBMatchContext();
            emailContext.setInputEmail((String) data[i][6]);
            emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
            emailContexts.put(String.valueOf(i), emailContext);
        }
        blackCaches = dnbCacheService.batchLookupBlackCache(emailContexts);
        Assert.assertEquals(blackCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBBlackCache blackCache = blackCaches.get(String.valueOf(i));
            Assert.assertNotNull(blackCache);
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
