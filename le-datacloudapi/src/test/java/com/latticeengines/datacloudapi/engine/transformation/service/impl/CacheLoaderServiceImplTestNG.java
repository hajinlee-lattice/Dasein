package com.latticeengines.datacloudapi.engine.transformation.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloudapi.engine.testframework.PropDataEngineAbstractTestNGBase;
import com.latticeengines.datacloudapi.engine.transformation.service.CacheLoaderConfig;
import com.latticeengines.datacloudapi.engine.transformation.service.CacheLoaderService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

@Component
public class CacheLoaderServiceImplTestNG extends PropDataEngineAbstractTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CacheLoaderServiceImplTestNG.class);

    private static final String podId = "CacheLoaderServiceImplTestNG";
    private static final String avroDir = "/tmp/CacheLoaderServiceImplTestNG";
    private static final String AM_CACHE_FILE = "am_cache.avro";

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
            config.setIsWhiteCache(true);
            long count = ((AvroCacheLoaderServiceImpl) cacheLoaderService).startLoad(avroDir, config);

            Assert.assertEquals(count, 83);
            assertCachePositiveWithDuns();
        } catch (Exception ex) {
            log.error("Exception!", ex);
            Assert.fail("Test failed! due to=" + ex.getMessage());
        }
    }

    private void assertCachePositiveWithDuns() {
        DnBMatchContext context = new DnBMatchContext();
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName("R. W. Notary");
        nameLocation.setCity("Alta Loma");
        nameLocation.setState("California");
        nameLocation.setCountryCode("USA");
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
    
    @Test(groups = "deployment", enabled = true)
    public void startLoadWithoutDuns() {
        try {
            HdfsPodContext.changeHdfsPodId(podId);
            uploadTestAVro(avroDir, AM_CACHE_FILE);
            CacheLoaderConfig config = new CacheLoaderConfig();
            config.setDirPath(avroDir);
            getFieldMap(config);
            config.setDunsField("DUNS");
            config.setConfidenceCode(4);
            config.setMatchGrade("AAZ");
            config.setIsWhiteCache(false);
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
        nameLocation.setCountryCode("USA");
        nameLocation.setZipcode(null);
        nameLocation.setPhoneNumber(null);
        nameLocationService.normalize(nameLocation);
        context.setInputNameLocation(nameLocation);
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBCache blackCache = dnbCacheService.lookupCache(context);

        Assert.assertTrue(blackCache != null);
        Assert.assertNull(blackCache.getDuns());
        Assert.assertNull(blackCache.getConfidenceCode());
        Assert.assertNull(blackCache.getMatchGrade());
    }

    private void uploadTestAVro(String avroDir, String fileName) {
        try {
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, String.format("sources/%s", fileName), avroDir + "/"
                    + fileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload test avro.", e);
        }
    }

}
