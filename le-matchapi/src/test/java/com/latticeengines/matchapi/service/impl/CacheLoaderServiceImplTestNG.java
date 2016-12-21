package com.latticeengines.matchapi.service.impl;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.matchapi.service.CacheLoaderConfig;
import com.latticeengines.matchapi.service.CacheLoaderService;
import com.latticeengines.matchapi.testframework.MatchapiFunctionalTestNGBase;

@Component
public class CacheLoaderServiceImplTestNG extends MatchapiFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(CacheLoaderServiceImplTestNG.class);

    private static final String podId = "CacheLoaderServiceImplTestNG";
    private static final String avroDir = "/tmp/CacheLoaderServiceImplTestNG";
    private static final String AM_CACHE_FILE = "am_cache.avro";

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private CacheLoaderService<GenericRecord> cacheLoaderService;

    @Autowired
    private DnBCacheService dnbCacheService;

    @Test(groups = "deployment", enabled = true)
    public void startLoad() {
        try {
            HdfsPodContext.changeHdfsPodId(podId);
            uploadTestAVro(avroDir, AM_CACHE_FILE);
            CacheLoaderConfig config = new CacheLoaderConfig();
            config.setDirPath(avroDir);
            long count = ((AvroCacheLoaderServiceImpl) cacheLoaderService).startLoad(avroDir, config);

            Assert.assertEquals(count, 83);
            assertCachePositive();
            assertCacheNegative();
        } catch (Exception ex) {
            log.error("Exception!", ex);
            Assert.fail("Test failed! due to=" + ex.getMessage());
        }
    }

    private void assertCacheNegative() {
        DnBMatchContext context = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName("Miaochafong");
        context.setInputNameLocation(nameLocation);

        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBWhiteCache whiteCache = dnbCacheService.lookupWhiteCache(context);
        Assert.assertTrue(whiteCache == null);

    }

    private void assertCachePositive() {

        DnBMatchContext context = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName("R. W. Notary");
        nameLocation.setCity("Alta Loma");
        nameLocation.setState("California");
        nameLocation.setCountryCode("USA");
        nameLocation.setZipcode("917374428");
        nameLocation.setPhoneNumber(null);
        context.setInputNameLocation(nameLocation);

        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBWhiteCache whiteCache = dnbCacheService.lookupWhiteCache(context);
        Assert.assertTrue(whiteCache != null);
        Assert.assertEquals(whiteCache.getDuns(), "039891115");
        Assert.assertEquals(whiteCache.getConfidenceCode(), new Integer(8));
        Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), "AAAAAAAAA");
    }

    private void uploadTestAVro(String avroDir, String fileName) {
        try {
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, String.format("accountmaster/%s", fileName), avroDir
                    + "/" + fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }

}
