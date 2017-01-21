package com.latticeengines.matchapi.service.impl;

import java.util.HashMap;
import java.util.Map;

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
import com.latticeengines.datacloud.match.dnb.DnBCache;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.datacloud.match.service.NameLocationService;
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

    @Autowired
    private NameLocationService nameLocationService;

    @Test(groups = "deployment", enabled = true)
    public void startLoadWithDuns() {
        try {
            HdfsPodContext.changeHdfsPodId(podId);
            uploadTestAVro(avroDir, AM_CACHE_FILE);
            CacheLoaderConfig config = new CacheLoaderConfig();
            config.setDirPath(avroDir);
            getFieldMap(config);
            config.setDunsField("DUNS");
            config.setConfidenceCode(6);
            config.setMatchGrade("AAA");
            long count = ((AvroCacheLoaderServiceImpl) cacheLoaderService).startLoad(avroDir, config);

            Assert.assertEquals(count, 83);
            assertCachePositiveWithDuns();
            assertCacheNegativeWithDuns();
        } catch (Exception ex) {
            log.error("Exception!", ex);
            Assert.fail("Test failed! due to=" + ex.getMessage());
        }
    }

    private void assertCacheNegativeWithDuns() {
        DnBMatchContext context = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName("Miaochafong");
        context.setInputNameLocation(nameLocation);

        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBCache whiteCache = dnbCacheService.lookupCache(context);
        Assert.assertTrue(whiteCache == null);

    }

    private void assertCachePositiveWithDuns() {

        DnBMatchContext context = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName("R. W. Notary");
        nameLocation.setCity("Alta Loma");
        nameLocation.setState("California");
        nameLocation.setCountry("USA");
        nameLocation.setZipcode("917374428");
        nameLocation.setPhoneNumber(null);
        nameLocationService.normalize(nameLocation);
        context.setInputNameLocation(nameLocation);
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBCache whiteCache = dnbCacheService.lookupCache(context);

        Assert.assertTrue(whiteCache != null);
        Assert.assertEquals(whiteCache.getDuns(), "039891115");
        Assert.assertEquals(whiteCache.getConfidenceCode(), new Integer(6));
        Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), "AAA");
    }

    @Test(groups = "deployment", enabled = true)
    public void startLoadWithoutDuns() {
        try {
            HdfsPodContext.changeHdfsPodId(podId);
            uploadTestAVro(avroDir, AM_CACHE_FILE);
            CacheLoaderConfig config = new CacheLoaderConfig();
            config.setDirPath(avroDir);
            getFieldMap(config);

            config.setDunsField("DUNS");
            config.setCallMatch(true);
            // config.setBatchMode(true);
            long count = ((AvroCacheLoaderServiceImpl) cacheLoaderService).startLoad(avroDir, config);

            Assert.assertEquals(count, 17);
            assertCachePositiveWithoutDuns();

        } catch (Exception ex) {
            log.error("Exception!", ex);
            Assert.fail("Test failed! due to=" + ex.getMessage());
        }
    }

    private void assertCachePositiveWithoutDuns() {

        DnBMatchContext context = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName("Angeles Grocery");
        nameLocation.setCity("Jersey City");
        nameLocation.setState("New Jersey");
        nameLocation.setCountry("USA");
        nameLocation.setZipcode(null);
        nameLocation.setPhoneNumber(null);
        nameLocationService.normalize(nameLocation);
        context.setInputNameLocation(nameLocation);
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBCache whiteCache = dnbCacheService.lookupCache(context);

        Assert.assertTrue(whiteCache != null);
        Assert.assertEquals(whiteCache.getDuns(), "149259751");

        Assert.assertEquals(whiteCache.getConfidenceCode(), new Integer(7));
        Assert.assertEquals(whiteCache.getMatchGrade().getRawCode(), "AZZAAZZZFBA");
    }

    private void getFieldMap(CacheLoaderConfig config) {
        Map<String, String> fieldMap = new HashMap<>();

        fieldMap.put("Name", "name");
        fieldMap.put("Country", "countryCode");
        fieldMap.put("State", "state");
        fieldMap.put("City", "city");
        fieldMap.put("PhoneNumber", "phoneNumber");
        fieldMap.put("ZipCode", "zipcode");
        config.setFieldMap(fieldMap);
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
