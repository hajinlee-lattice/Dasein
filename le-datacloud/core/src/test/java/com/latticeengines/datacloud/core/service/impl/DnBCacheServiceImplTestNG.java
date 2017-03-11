package com.latticeengines.datacloud.core.service.impl;

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

import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public class DnBCacheServiceImplTestNG extends DataCloudCoreFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(DnBCacheServiceImplTestNG.class);

    @Autowired
    private DnBCacheService dnbCacheService;

    private List<DnBCache> whiteCaches = new ArrayList<DnBCache>();

    private List<DnBCache> blackCaches = new ArrayList<DnBCache>();

    @AfterClass(groups = "functional", enabled = true)
    public void afterClass() {
        deleteEntities();
    }

    private void deleteEntities() {
        Assert.assertTrue(!whiteCaches.isEmpty());
        for (DnBCache whiteCache : whiteCaches) {
            dnbCacheService.getCacheMgr().delete(whiteCache);
        }
        Assert.assertTrue(!blackCaches.isEmpty());
        for (DnBCache blackCache : blackCaches) {
            dnbCacheService.getCacheMgr().delete(blackCache);
        }
    }
    
    /*********************************
     * White Cache
     *********************************/

    @Test(groups = "functional", enabled = true)
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
        entityContext.setOutOfBusiness((Boolean) data[0][10]);
        entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        entityContext.setDnbCode(DnBReturnCode.OK);
        whiteCaches.add(dnbCacheService.addCache(entityContext));
        
        DnBMatchContext emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[0][6]);
        emailContext.setDuns((String) data[0][7]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        emailContext.setDnbCode(DnBReturnCode.OK);
        whiteCaches.add(dnbCacheService.addCache(emailContext));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateWhiteCache", enabled = true)
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
            entityContext.setOutOfBusiness((Boolean) data[i][10]);
            entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
            entityContext.setDnbCode(DnBReturnCode.OK);
            entityContexts.add(entityContext);
        }
        whiteCaches.addAll(dnbCacheService.batchAddCache(entityContexts));

        List<DnBMatchContext> emailContexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < data.length - 1; i++) {
            DnBMatchContext emailContext = new DnBMatchContext();
            emailContext.setInputEmail((String) data[i][6]);
            emailContext.setDuns((String) data[i][7]);
            emailContext.setConfidenceCode((Integer) data[i][8]);
            emailContext.setMatchGrade((String) data[i][9]);
            emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
            emailContext.setDnbCode(DnBReturnCode.OK);
            emailContexts.add(emailContext);
        }
        whiteCaches.addAll(dnbCacheService.batchAddCache(emailContexts));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWhiteCache",
            "testBatchCreateWhiteCache" }, enabled = true)
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
        DnBCache whiteCache = dnbCacheService.lookupCache(entityContext);
        Assert.assertEquals(whiteCache.getDuns(), (String) data[0][7]);
        Assert.assertEquals(whiteCache.getConfidenceCode(), (Integer) data[0][8]);
        Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), (String) data[0][9]);
        Assert.assertEquals(whiteCache.isOutOfBusiness(), (Boolean) data[0][10]);
        Assert.assertNotNull(whiteCache.getTimestamp());
        log.info(String.format("Timestamp of white cache: %d", whiteCache.getTimestamp()));

        DnBMatchContext emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[0][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        whiteCache = dnbCacheService.lookupCache(emailContext);
        Assert.assertEquals(whiteCache.getDuns(), (String) data[0][7]);

        // cache miss
        entityContext = new DnBMatchContext();
        entityContext.getInputNameLocation().setName((String) data[data.length - 1][0]);
        entityContext.getInputNameLocation().setCountryCode((String) data[data.length - 1][1]);
        entityContext.getInputNameLocation().setState((String) data[data.length - 1][2]);
        entityContext.getInputNameLocation().setCity((String) data[data.length - 1][3]);
        entityContext.getInputNameLocation().setPhoneNumber((String) data[data.length - 1][4]);
        entityContext.getInputNameLocation().setZipcode((String) data[data.length - 1][5]);
        entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        Assert.assertNull(dnbCacheService.lookupCache(entityContext));

        emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[data.length - 1][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        Assert.assertNull(dnbCacheService.lookupCache(emailContext));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWhiteCache",
            "testBatchCreateWhiteCache" }, enabled = true)
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
            entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
            entityContexts.put(String.valueOf(i), entityContext);
        }
        Map<String, DnBCache> whiteCaches = dnbCacheService.batchLookupCache(entityContexts);
        Assert.assertEquals(whiteCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBCache whiteCache = whiteCaches.get(String.valueOf(i));
            Assert.assertNotNull(whiteCache);
            Assert.assertEquals(whiteCache.getDuns(), (String) data[i][7]);
            Assert.assertEquals(whiteCache.getConfidenceCode(), (Integer) data[i][8]);
            Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), (String) data[i][9]);
            Assert.assertEquals(whiteCache.isOutOfBusiness(), (Boolean) data[i][10]);
        }
        Assert.assertTrue(!whiteCaches.containsKey(String.valueOf(data.length - 1)));

        Map<String, DnBMatchContext> emailContexts = new HashMap<String, DnBMatchContext>();
        for (int i = 1; i < data.length; i++) {
            DnBMatchContext emailContext = new DnBMatchContext();
            emailContext.setInputEmail((String) data[i][6]);
            emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
            emailContexts.put(String.valueOf(i), emailContext);
        }
        whiteCaches = dnbCacheService.batchLookupCache(emailContexts);
        Assert.assertEquals(whiteCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBCache whiteCache = whiteCaches.get(String.valueOf(i));
            Assert.assertNotNull(whiteCache);
            Assert.assertEquals(whiteCache.getDuns(), (String) data[i][7]);
        }
        Assert.assertTrue(!whiteCaches.containsKey(String.valueOf(data.length - 1)));
    }

    /*********************************
     * Black Cache
     *********************************/

    @Test(groups = "functional", enabled = true)
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
        entityContext.setDnbCode(DnBReturnCode.UNMATCH);
        blackCaches.add(dnbCacheService.addCache(entityContext));

        DnBMatchContext emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[0][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        emailContext.setDnbCode(DnBReturnCode.UNMATCH);
        blackCaches.add(dnbCacheService.addCache(emailContext));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateBlackCache", enabled = true)
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
            entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
            entityContext.setDnbCode(DnBReturnCode.UNMATCH);
            entityContexts.add(entityContext);
        }
        blackCaches.addAll(dnbCacheService.batchAddCache(entityContexts));

        List<DnBMatchContext> emailContexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < data.length - 1; i++) {
            DnBMatchContext emailContext = new DnBMatchContext();
            emailContext.setInputEmail((String) data[i][6]);
            emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
            emailContext.setDnbCode(DnBReturnCode.UNMATCH);
            emailContexts.add(emailContext);
        }
        blackCaches.addAll(dnbCacheService.batchAddCache(emailContexts));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateBlackCache",
            "testBatchCreateBlackCache" }, enabled = true)
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
        DnBCache blackCache = dnbCacheService.lookupCache(entityContext);
        Assert.assertNotNull(blackCache);
        Assert.assertNotNull(blackCache.getTimestamp());
        log.info(String.format("Timestamp of black cache: %s", blackCache.getTimestamp()));

        DnBMatchContext emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[0][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        blackCache = dnbCacheService.lookupCache(emailContext);
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
        Assert.assertNull(dnbCacheService.lookupCache(entityContext));

        emailContext = new DnBMatchContext();
        emailContext.setInputEmail((String) data[data.length - 1][6]);
        emailContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.EMAIL);
        Assert.assertNull(dnbCacheService.lookupCache(emailContext));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateBlackCache",
            "testBatchCreateBlackCache" }, enabled = true)
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
            entityContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
            entityContexts.put(String.valueOf(i), entityContext);
        }
        Map<String, DnBCache> blackCaches = dnbCacheService.batchLookupCache(entityContexts);
        Assert.assertEquals(blackCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBCache blackCache = blackCaches.get(String.valueOf(i));
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
        blackCaches = dnbCacheService.batchLookupCache(emailContexts);
        Assert.assertEquals(blackCaches.size(), data.length - 2);
        for (int i = 1; i < data.length - 1; i++) {
            DnBCache blackCache = blackCaches.get(String.valueOf(i));
            Assert.assertNotNull(blackCache);
        }
        Assert.assertTrue(!blackCaches.containsKey(String.valueOf(data.length - 1)));
    }

    private static Object[][] getEntityInputData() {
        return new Object[][] {
                { "DUMMY_NAME1", "DUMMY_COUNTRYCODE1", "DUMMY_STATE1", "DUMMY_CITY1", "DUMMY_PHONE1", "DUMMY_ZIPCODE1",
                        "DUMMY_EMAIL1", "DUMMY_DUNS1", 7, "DUMMY_MATCHGRADE1", Boolean.TRUE },
                { "DUMMY_NAME2", "DUMMY_COUNTRYCODE2", "DUMMY_STATE2", "DUMMY_CITY2", "DUMMY_PHONE2", "DUMMY_ZIPCODE2",
                        "DUMMY_EMAIL2", "DUMMY_DUNS2", 5, "DUMMY_MATCHGRADE2", Boolean.FALSE },
                { "DUMMY_NAME3", "DUMMY_COUNTRYCODE3", "DUMMY_STATE3", "DUMMY_CITY3", "DUMMY_PHONE3", "DUMMY_ZIPCODE3",
                        "DUMMY_EMAIL3", "DUMMY_DUNS3", 6, "DUMMY_MATCHGRADE3", null },
                { "DUMMY_NAME4", "DUMMY_COUNTRYCODE4", "DUMMY_STATE4", "DUMMY_CITY4", "DUMMY_PHONE4", "DUMMY_ZIPCODE4",
                        "DUMMY_EMAIL4", "DUMMY_DUNS4", 6, "DUMMY_MATCHGRADE4", null } };
    }
}
