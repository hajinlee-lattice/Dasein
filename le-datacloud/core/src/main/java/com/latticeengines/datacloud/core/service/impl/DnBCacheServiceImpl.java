package com.latticeengines.datacloud.core.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DnBCacheEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.impl.DnBCacheEntityMgrImpl;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;

@Component("dnbCacheService")
public class DnBCacheServiceImpl implements DnBCacheService {
    private static final Logger log = LoggerFactory.getLogger(DnBCacheServiceImpl.class);

    @Value("${datacloud.dnb.cache.version}")
    private String cacheVersion;

    @Value("${datacloud.dnb.cache.white.expire.days}")
    private long whiteExpireDays;

    @Value("${datacloud.dnb.cache.black.expire.days}")
    private long blackExpireDays;

    @Value("${datacloud.dnb.cache.notinam.expire.days}")
    private long notinamExpireDays;

    @Value("${datacloud.dnb.cache.expire.factor}")
    private double expireFactor;

    private Map<String, DnBCacheEntityMgr> cacheEntityMgrs = new HashMap<String, DnBCacheEntityMgr>();

    @Inject
    private FabricDataService dataService;

    private final List<DnBCache> cacheQueue = Collections.synchronizedList(new ArrayList<>());

    @Resource(name = "commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    private static final int BUFFER_SIZE = 25;

    @Value("${datacloud.dnb.cache.queue.size}")
    private int queueMaxSize;

    @PostConstruct
    public void postConstruct() {
        scheduler.scheduleWithFixedDelay(this::dumpQueue, TimeUnit.SECONDS.toMillis(5));
    }

    @Override
    public void dumpQueue() {
        List<DnBCache> caches = new ArrayList<>();
        synchronized (cacheQueue) {
            caches.addAll(cacheQueue);
            cacheQueue.clear();
        }
        if (!caches.isEmpty()) {
            Long startTime = System.currentTimeMillis();
            List<DnBCache> cacheBuffer = new ArrayList<>();
            log.info("Splitting " + caches.size() + " DnBCaches into groups.");
            for (DnBCache cache : caches) {
                cacheBuffer.add(cache);
                if (cacheBuffer.size() >= BUFFER_SIZE) {
                    getCacheMgr().batchCreate(cacheBuffer);
                    cacheBuffer.clear();
                }
            }
            if (!cacheBuffer.isEmpty()) {
                getCacheMgr().batchCreate(cacheBuffer);
                cacheBuffer.clear();
            }
            log.info(String.format("Dumped %d DnBCaches into Dynamo table. Duration=%d", caches.size(),
                    System.currentTimeMillis() - startTime));
        }
    }

    @Override
    public DnBCache lookupCache(DnBMatchContext context) {
        DnBCache input = initCacheEntity(context, true);
        DnBCache output = getCacheMgr().findByKey(input);
        if (output != null) {
            output.parseCacheContext();
        }
        if (expire(output)) {
            return null;
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
    public DnBCache addCache(DnBMatchContext context, boolean sync) {
        DnBCache cache = null;
        if (StringUtils.isNotEmpty(context.getDuns())
                && (context.getDnbCode() == DnBReturnCode.OK || context.getDnbCode() == DnBReturnCode.DISCARD)) {
            cache = initCacheEntity(context, false);
            cache.setWhiteCache(true);
        } else if (context.getDnbCode() == DnBReturnCode.UNMATCH) {
            cache = initCacheEntity(context, true);
            cache.setWhiteCache(false);
        } else {
            return null;
        }
        if (context.getPatched() != null) {
            cache.setPatched(context.getPatched());
        }
        if (sync) {
            getCacheMgr().create(cache);
            log.info(String.format("Added Id=%s to %s DnBCache. DUNS=%s, DnBCode=%s, OutOfBusiness=%s, DunsInAM=%s",
                    cache.getId(), cache.isWhiteCache() ? "white" : "black", context.getDuns(), context.getDnbCode(),
                    context.isOutOfBusinessString(), context.isDunsInAMString()));
        } else {
            if (cacheQueue.size() >= queueMaxSize) {
                log.info(String.format(
                        "Discarded Id=%s since DnBCache queue has exceeded maximum size limit %d. DnBCode=%s. OutOfBusiness=%s, DunsInAM=%s",
                        cache.getId(), queueMaxSize, context.getDnbCode(), context.isOutOfBusinessString(),
                        context.isDunsInAMString()));
            } else {
                cacheQueue.add(cache);
                log.info(String.format(
                        "Added Id=%s to %s DnBCache queue. DUNS=%s, DnBCode=%s, OutOfBusiness=%s, DunsInAM=%s",
                        cache.getId(), cache.isWhiteCache() ? "white" : "black", context.getDuns(),
                        context.getDnbCode(), context.isOutOfBusinessString(), context.isDunsInAMString()));
            }
        }
        return cache;
    }

    @Override
    public List<DnBCache> batchAddCache(List<DnBMatchContext> contexts) {
        List<DnBCache> caches = new ArrayList<DnBCache>();
        for (DnBMatchContext context : contexts) {
            DnBCache cache = null;
            if (StringUtils.isNotEmpty(context.getDuns())
                    && (context.getDnbCode() == DnBReturnCode.OK || context.getDnbCode() == DnBReturnCode.DISCARD)) {
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
            log.info(String.format("Added Id=%s to %s cache. DnBCode=%s. OutOfBusiness=%s, DunsInAM=%s", cache.getId(),
                    cache.isWhiteCache() ? "white" : "black", context.getDnbCode(),
                    context.isOutOfBusinessString(), context.isDunsInAMString()));
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
            log.info("Use " + cacheVersion + " as full version of DnBCache for " + cacheVersion);
            cacheEntityMgr = new DnBCacheEntityMgrImpl(dataService, cacheVersion);
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
                        context.getMatchGrade(), context.getMatchedNameLocation(), context.isOutOfBusiness(),
                        context.isDunsInAM());
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
                        context.getMatchGrade(), context.getMatchedNameLocation(), context.isOutOfBusiness(),
                        context.isDunsInAM());
            }
        default:
            throw new UnsupportedOperationException(
                    "DnBCache.CacheType " + context.getMatchStrategy().name() + " is supported in DnB cache lookup");
        }
    }

    private boolean expire(DnBCache cache) {
        if (cache == null) {
            return false;
        }
        long currentDays = System.currentTimeMillis() / DnBCache.DAY_IN_MILLIS;
        // Black cache
        if (!cache.isWhiteCache()) {
            if (cache.getTimestamp() != null && currentDays <= (cache.getTimestamp().longValue() + blackExpireDays)) {
                return false;
            } else {
                return tryExpire(cache);
            }
        } else if (Boolean.FALSE.equals(cache.isDunsInAM())) {  // White cache, but Duns not in AM
            if (cache.getTimestamp() != null && currentDays <= (cache.getTimestamp().longValue() + notinamExpireDays)) {
                return false;
            } else {
                return tryExpire(cache);
            }
        } else { // Other white cache
            if (cache.getTimestamp() != null && currentDays <= (cache.getTimestamp().longValue() + whiteExpireDays)) {
                return false;
            } else {
                return tryExpire(cache);
            }
        }
    }

    private boolean tryExpire(DnBCache cache) {
        double factor = Math.random();
        if (factor <= expireFactor) {
            log.info(String.format(
                    "Cache is expired: Id=%s, IsWhiteCache=%b, IsDunsInAM=%s, Cache Timestamp (in day)=%d, Current Timestamp (in day)=%d, Calculated expire factor=%f",
                    cache.getId(), cache.isWhiteCache(), cache.isDunsInAMString(), cache.getTimestamp().longValue(),
                    System.currentTimeMillis() / DnBCache.DAY_IN_MILLIS, factor));
            return true;
        } else {
            return false;
        }
    }

    public int getQueueSize() {
        return cacheQueue.size();
    }

}
