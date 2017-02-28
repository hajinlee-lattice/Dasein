package com.latticeengines.datacloud.core.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.DnBCacheEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.impl.DnBCacheEntityMgrImpl;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

@Component("dnbCacheService")
public class DnBCacheServiceImpl implements DnBCacheService {
    private static final Log log = LogFactory.getLog(DnBCacheServiceImpl.class);

    @Value("${datacloud.dnb.cache.version}")
    private String cacheVersion;

    @Value("${datacloud.dnb.cache.white.expire.days}")
    private long whiteCacheExpireDays;

    @Value("${datacloud.dnb.cache.black.expire.days}")
    private long blackCacheExpireDays;

    @Value("${datacloud.dnb.cache.expire.factor}")
    private double expireFactor;

    private Map<String, DnBCacheEntityMgr> cacheEntityMgrs = new HashMap<String, DnBCacheEntityMgr>();

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;

    @Override
    public DnBCache lookupCache(DnBMatchContext context) {
        DnBCache input = initCacheEntity(context, true);
        DnBCache output = getCacheMgr().findByKey(input);
        if (expire(output)) {
            return null;
        }
        if (output != null) {
            output.parseCacheContext();
        }
        return output;
    }

    @Override
    public Map<String, DnBCache> batchLookupCache(Map<String, DnBMatchContext> contexts) {
        List<String> keys = new ArrayList<String>();
        List<String> lookupRequestIds = new ArrayList<String>();
        for (String lookupRequestId : contexts.keySet()) {
            DnBCache input = initCacheEntity(contexts.get(lookupRequestId), true);
            keys.add(input.getId());
            lookupRequestIds.add(lookupRequestId);
        }
        List<DnBCache> outputs = getCacheMgr().batchFindByKey(keys);
        Map<String, DnBCache> result = new HashMap<String, DnBCache>();
        for (int i = 0; i < outputs.size(); i++) {
            DnBCache output = outputs.get(i);
            if (expire(output)) {
                output = null;
            }
            if (output != null) {
                output.parseCacheContext();
                result.put(lookupRequestIds.get(i), output);
            }
        }
        return result;
    }

    @Override
    public DnBCache addCache(DnBMatchContext context) {
        DnBCache cache = null;
        if (context.getDnbCode() == DnBReturnCode.OK || context.getDnbCode() == DnBReturnCode.DISCARD) {
            if (!context.isOutOfBusiness()) {
                cache = initCacheEntity(context, false);
                cache.setWhiteCache(true);
            } else {
                cache = initCacheEntity(context, true);
                cache.setWhiteCache(false);
            }
        } else if (context.getDnbCode() == DnBReturnCode.UNMATCH) {
            cache = initCacheEntity(context, true);
            cache.setWhiteCache(false);
        } else {
            return null;
        }
        if (context.getPatched() != null) {
            cache.setPatched(context.getPatched());
        }
        getCacheMgr().create(cache);
        log.info(String.format("Added Id = %s to %s cache. DnBCode = %s. OutOfBusiness = %b", cache.getId(),
                cache.isWhiteCache() ? "white" : "black", context.getDnbCode().getMessage(),
                context.isOutOfBusiness()));
        return cache;
    }

    @Override
    public List<DnBCache> batchAddCache(List<DnBMatchContext> contexts) {
        List<DnBCache> caches = new ArrayList<DnBCache>();
        for (DnBMatchContext context : contexts) {
            DnBCache cache = null;
            if (context.getDnbCode() == DnBReturnCode.OK || context.getDnbCode() == DnBReturnCode.DISCARD) {
                cache = initCacheEntity(context, false);
                cache.setWhiteCache(true);
            } else if (context.getDnbCode() == DnBReturnCode.UNMATCH) {
                cache = initCacheEntity(context, true);
                cache.setWhiteCache(false);
            } else {
                continue;
            }
            if (context.getPatched() != null) {
                cache.setPatched(context.getPatched());
            }
            caches.add(cache);
            log.info(String.format("Added Id = %s to %s cache", cache.getId(),
                    cache.isWhiteCache() ? "white" : "black"));
        }
        getCacheMgr().batchCreate(caches);
        return caches;
    }

    @Override
    public void removeCache(DnBCache cache) {
        getCacheMgr().delete(cache);
        log.info("Removed Id=" + cache.getId() + " from cache.");
    }

    @Override
    public DnBCacheEntityMgr getCacheMgr() {
        DnBCacheEntityMgr cacheEntityMgr = cacheEntityMgrs.get(cacheVersion);
        if (cacheEntityMgr == null)
            cacheEntityMgr = getCacheMgrSync();
        return cacheEntityMgr;
    }

    private synchronized DnBCacheEntityMgr getCacheMgrSync() {
        DnBCacheEntityMgr cacheEntityMgr = cacheEntityMgrs.get(cacheVersion);

        if (cacheEntityMgr == null) {
            DataCloudVersion dataCloudVersion = versionEntityMgr.findVersion(cacheVersion);
            if (dataCloudVersion == null) {
                throw new IllegalArgumentException("Cannot find the specified data cloud version " + cacheVersion);
            }
            log.info("Use " + cacheVersion + " as full version of DnBCache for " + cacheVersion);
            cacheEntityMgr = new DnBCacheEntityMgrImpl(messageService, dataService, cacheVersion);
            cacheEntityMgr.init();
            cacheEntityMgrs.put(cacheVersion, cacheEntityMgr);
        }

        return cacheEntityMgr;
    }

    private DnBCache initCacheEntity(DnBMatchContext context, boolean noMatchedContext) {
        switch (context.getMatchStrategy()) {
        case ENTITY:
            if (noMatchedContext) {
                return new DnBCache(context.getInputNameLocation());
            } else {
                return new DnBCache(context.getInputNameLocation(), context.getDuns(), context.getConfidenceCode(),
                        context.getMatchGrade(), context.getMatchedNameLocation());
            }
        case EMAIL:
            if (noMatchedContext) {
                return new DnBCache(context.getInputEmail());
            } else {
                return new DnBCache(context.getInputEmail(), context.getDuns());
            }
        case BATCH:
            if (noMatchedContext) {
                return new DnBCache(context.getInputNameLocation());
            } else {
                return new DnBCache(context.getInputNameLocation(), context.getDuns(), context.getConfidenceCode(),
                        context.getMatchGrade(), context.getMatchedNameLocation());
            }
        default:
            throw new UnsupportedOperationException(
                    "DnBCache.CacheType " + context.getMatchStrategy().name() + " is supported in DnB cache lookup");
        }
    }

    private boolean expire(DnBCache cache) {
        long currentDays = System.currentTimeMillis() / DnBCache.DAY_IN_MILLIS;
        if (cache.isWhiteCache() && cache.getTimestamp() != null
                && currentDays <= (cache.getTimestamp().longValue() + whiteCacheExpireDays)) {
            return false;
        }
        if (!cache.isWhiteCache() && cache.getTimestamp() != null
                && currentDays <= (cache.getTimestamp().longValue() + blackCacheExpireDays)) {
            return false;
        }
        if (Math.random() <= expireFactor) {
            log.info(
                    String.format("Cache is expired: Id = %s, IsWhiteCache = %b", cache.getId(), cache.isWhiteCache()));
            removeCache(cache);
            return true;
        } else {
            return false;
        }
    }

}
