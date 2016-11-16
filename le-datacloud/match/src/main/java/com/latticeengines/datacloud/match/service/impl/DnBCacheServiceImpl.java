package com.latticeengines.datacloud.match.service.impl;

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
import com.latticeengines.datacloud.match.dnb.DnBBlackCache;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.entitymgr.DnBBlackCacheEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.DnBWhiteCacheEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.DnBBlackCacheEntityMgrImpl;
import com.latticeengines.datacloud.match.entitymgr.impl.DnBWhiteCacheEntityMgrImpl;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

@Component("dnbCacheService")
public class DnBCacheServiceImpl implements DnBCacheService {
    private static final Log log = LogFactory.getLog(DnBCacheServiceImpl.class);

    @Value("${datacloud.dnb.cache.version}")
    private String cacheVersion;

    private Map<String, DnBWhiteCacheEntityMgr> whiteCacheEntityMgrs = new HashMap<String, DnBWhiteCacheEntityMgr>();

    private Map<String, DnBBlackCacheEntityMgr> blackCacheEntityMgrs = new HashMap<String, DnBBlackCacheEntityMgr>();

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;

    /*********************************
     * White Cache
     *********************************/

    @Override
    public DnBWhiteCache lookupWhiteCache(DnBMatchContext context) {
        DnBWhiteCache input = initWhiteCacheEntity(context, true);
        DnBWhiteCache output = getWhiteCacheMgr().findByKey(input);
        if (output != null) {
            output.parseCacheContext();
        }
        return output;
    }

    @Override
    public Map<String, DnBWhiteCache> batchLookupWhiteCache(Map<String, DnBMatchContext> contexts) {
        List<String> keys = new ArrayList<String>();
        List<String> lookupRequestIds = new ArrayList<String>();
        for (String lookupRequestId : contexts.keySet()) {
            DnBWhiteCache input = initWhiteCacheEntity(contexts.get(lookupRequestId), true);
            keys.add(input.getId());
            lookupRequestIds.add(lookupRequestId);
        }
        List<DnBWhiteCache> outputs = getWhiteCacheMgr().batchFindByKey(keys);
        Map<String, DnBWhiteCache> result = new HashMap<String, DnBWhiteCache>();
        for (int i = 0; i < outputs.size(); i++) {
            DnBWhiteCache output = outputs.get(i);
            if (output != null) {
                output.parseCacheContext();
                result.put(lookupRequestIds.get(i), output);
            }
        }
        return result;
    }

    @Override
    public DnBWhiteCache addWhiteCache(DnBMatchContext context) {
        DnBWhiteCache cache = initWhiteCacheEntity(context, false);
        getWhiteCacheMgr().create(cache);
        return cache;
    }

    @Override
    public List<DnBWhiteCache> batchAddWhiteCache(List<DnBMatchContext> contexts) {
        List<DnBWhiteCache> caches = new ArrayList<DnBWhiteCache>();
        for(DnBMatchContext context : contexts) {
            DnBWhiteCache cache = initWhiteCacheEntity(context, false);
            caches.add(cache);
        }
        getWhiteCacheMgr().batchCreate(caches);
        return caches;
    }

    @Override
    public DnBWhiteCacheEntityMgr getWhiteCacheMgr() {
        DnBWhiteCacheEntityMgr whiteCacheEntityMgr = whiteCacheEntityMgrs.get(cacheVersion);
        if (whiteCacheEntityMgr == null)
            whiteCacheEntityMgr = getWhiteCacheMgrSync();
        return whiteCacheEntityMgr;
    }

    private synchronized DnBWhiteCacheEntityMgr getWhiteCacheMgrSync() {
        DnBWhiteCacheEntityMgr whiteCacheEntityMgr = whiteCacheEntityMgrs.get(cacheVersion);

        if (whiteCacheEntityMgr == null) {
            DataCloudVersion dataCloudVersion = versionEntityMgr.findVersion(cacheVersion);
            if (dataCloudVersion == null) {
                throw new IllegalArgumentException("Cannot find the specified data cloud version " + cacheVersion);
            }
            log.info("Use " + cacheVersion + " as full version of DnBWhiteCache for " + cacheVersion);
            whiteCacheEntityMgr = new DnBWhiteCacheEntityMgrImpl(messageService, dataService, cacheVersion);
            whiteCacheEntityMgr.init();
            whiteCacheEntityMgrs.put(cacheVersion, whiteCacheEntityMgr);
        }

        return whiteCacheEntityMgr;
    }

    private DnBWhiteCache initWhiteCacheEntity(DnBMatchContext context, boolean lookup) {
        switch (context.getMatchStrategy()) {
        case ENTITY:
            if (lookup) {
                return new DnBWhiteCache(context.getInputNameLocation());
            } else {
                return new DnBWhiteCache(context.getInputNameLocation(), context.getDuns(), context.getConfidenceCode(),
                        context.getMatchGrade());
            }
        case EMAIL:
            if (lookup) {
                return new DnBWhiteCache(context.getInputEmail());
            } else {
                return new DnBWhiteCache(context.getInputEmail(), context.getDuns(), context.getConfidenceCode(),
                        context.getMatchGrade());
            }
        default:
            throw new UnsupportedOperationException("DnBWhiteCache.CacheType " + context.getMatchStrategy().name()
                    + " is supported in DnB white cache lookup");
        }
    }

    /*********************************
     * Black Cache
     *********************************/

    @Override
    public DnBBlackCache lookupBlackCache(DnBMatchContext context) {
        DnBBlackCache input = initBlackCacheEntity(context);
        DnBBlackCache output = getBlackCacheMgr().findByKey(input);
        return output;
    }

    @Override
    public Map<String, DnBBlackCache> batchLookupBlackCache(Map<String, DnBMatchContext> contexts) {
        List<String> keys = new ArrayList<String>();
        List<String> lookupRequestIds = new ArrayList<String>();
        for (String lookupRequestId : contexts.keySet()) {
            DnBBlackCache input = initBlackCacheEntity(contexts.get(lookupRequestId));
            keys.add(input.getId());
            lookupRequestIds.add(lookupRequestId);
        }
        List<DnBBlackCache> outputs = getBlackCacheMgr().batchFindByKey(keys);
        Map<String, DnBBlackCache> result = new HashMap<String, DnBBlackCache>();
        for (int i = 0; i < outputs.size(); i++) {
            DnBBlackCache output = outputs.get(i);
            if (output != null) {
                result.put(lookupRequestIds.get(i), output);
            }
        }
        return result;
    }

    @Override
    public DnBBlackCache addBlackCache(DnBMatchContext context) {
        DnBBlackCache cache = initBlackCacheEntity(context);
        getBlackCacheMgr().create(cache);
        return cache;
    }

    @Override
    public List<DnBBlackCache> batchAddBlackCache(List<DnBMatchContext> contexts) {
        List<DnBBlackCache> caches = new ArrayList<DnBBlackCache>();
        for (DnBMatchContext context : contexts) {
            DnBBlackCache cache = initBlackCacheEntity(context);
            caches.add(cache);
        }
        getBlackCacheMgr().batchCreate(caches);
        return caches;
    }

    @Override
    public DnBBlackCacheEntityMgr getBlackCacheMgr() {
        DnBBlackCacheEntityMgr blackCacheEntityMgr = blackCacheEntityMgrs.get(cacheVersion);
        if (blackCacheEntityMgr == null)
            blackCacheEntityMgr = getBlackCacheMgrSync();
        return blackCacheEntityMgr;
    }

    private synchronized DnBBlackCacheEntityMgr getBlackCacheMgrSync() {
        DnBBlackCacheEntityMgr blackCacheEntityMgr = blackCacheEntityMgrs.get(cacheVersion);

        if (blackCacheEntityMgr == null) {
            DataCloudVersion dataCloudVersion = versionEntityMgr.findVersion(cacheVersion);
            if (dataCloudVersion == null) {
                throw new IllegalArgumentException("Cannot find the specified data cloud version " + cacheVersion);
            }
            log.info("Use " + cacheVersion + " as full version of DnBBlackCache for " + cacheVersion);
            blackCacheEntityMgr = new DnBBlackCacheEntityMgrImpl(messageService, dataService, cacheVersion);
            blackCacheEntityMgr.init();
            blackCacheEntityMgrs.put(cacheVersion, blackCacheEntityMgr);
        }

        return blackCacheEntityMgr;
    }

    private DnBBlackCache initBlackCacheEntity(DnBMatchContext context) {
        switch (context.getMatchStrategy()) {
        case ENTITY:
            return new DnBBlackCache(context.getInputNameLocation());
        case EMAIL:
            return new DnBBlackCache(context.getInputEmail());
        default:
            throw new UnsupportedOperationException("DnBBlackCache.CacheType " + context.getMatchStrategy().name()
                    + " is supported in DnB black cache lookup");
        }
    }
}
