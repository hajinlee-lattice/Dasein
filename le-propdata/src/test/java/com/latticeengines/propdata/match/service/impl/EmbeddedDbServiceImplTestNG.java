package com.latticeengines.propdata.match.service.impl;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

public class EmbeddedDbServiceImplTestNG extends PropDataMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(EmbeddedDbServiceImplTestNG.class);

    @Autowired
    private EmbeddedDbServiceImpl embeddedDbService;

    @Autowired
    @Qualifier("embeddedJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @BeforeClass(groups = "functional")
    public void setup() {
        embeddedDbService.setChunckSize(10);
        embeddedDbService.setNumThreads(4);
    }

    @BeforeMethod(groups = "functional")
    public void setupMethod() {
        embeddedDbService.invalidate();
    }

    @Test(groups = "functional")
    public void testLoadMatchKeys() {
        Assert.assertEquals((Integer) EmbeddedDbServiceImpl.STATUS.get(), EmbeddedDbServiceImpl.UNAVAILABLE);
        Assert.assertFalse(embeddedDbService.isReady());

        embeddedDbService.loadSync(1000);

        Assert.assertEquals((Integer) EmbeddedDbServiceImpl.STATUS.get(), EmbeddedDbServiceImpl.READY);
        Assert.assertTrue(embeddedDbService.isReady());

        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + EmbeddedDbServiceImpl.MATCH_KEY_CACHE, Integer.class);
        Assert.assertTrue(count >= 1000);

        Map<String, Object> map = jdbcTemplate.queryForMap("SELECT TOP 1 * FROM " + EmbeddedDbServiceImpl.MATCH_KEY_CACHE);
        Assert.assertTrue(map.keySet().contains("LatticeAccountID"));
        Assert.assertTrue(map.keySet().contains("Domain"));
        Assert.assertTrue(map.keySet().contains("Name"));
        Assert.assertTrue(map.keySet().contains("City"));
        Assert.assertTrue(map.keySet().contains("State"));
        Assert.assertTrue(map.keySet().contains("Country"));
    }

    @Test(groups = "functional")
    public void testLoadMatchKeysAsync() throws InterruptedException {
        Assert.assertEquals((Integer) EmbeddedDbServiceImpl.STATUS.get(), EmbeddedDbServiceImpl.UNAVAILABLE);
        Assert.assertFalse(embeddedDbService.isReady());

        embeddedDbService.loadAsync();

        log.info("Wait 10 sec for the loading to start.");
        Thread.sleep(10000L);

        Assert.assertEquals((Integer) EmbeddedDbServiceImpl.STATUS.get(), EmbeddedDbServiceImpl.LOADING);
        Assert.assertFalse(embeddedDbService.isReady());

        embeddedDbService.invalidate();

        log.info("Wait 3 sec for the cancelling to take effect.");
        Thread.sleep(3000L);

        Integer count1 = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + EmbeddedDbServiceImpl.MATCH_KEY_CACHE, Integer.class);

        log.info("Wait 5 sec to count again.");
        Thread.sleep(5000L);

        Integer count2 = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + EmbeddedDbServiceImpl.MATCH_KEY_CACHE, Integer.class);
        Assert.assertEquals(count1, count2, "Count should not increase after cancelling the loading");

        Assert.assertEquals((Integer) EmbeddedDbServiceImpl.STATUS.get(), EmbeddedDbServiceImpl.UNAVAILABLE);
        Assert.assertFalse(embeddedDbService.isReady());
    }
}
