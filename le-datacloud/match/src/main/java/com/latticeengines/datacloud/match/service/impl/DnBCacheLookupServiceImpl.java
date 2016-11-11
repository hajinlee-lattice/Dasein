package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.entitymgr.DnBWhiteCacheEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.DnBWhiteCacheEntityMgrImpl;
import com.latticeengines.datacloud.match.service.DnBCacheLookupService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

@Component("dnbCacheLookupService")
public class DnBCacheLookupServiceImpl implements DnBCacheLookupService {
    private static final Log log = LogFactory.getLog(DnBCacheLookupServiceImpl.class);

    private Map<String, DnBWhiteCacheEntityMgr> whiteCacheEntityMgrs = new HashMap<String, DnBWhiteCacheEntityMgr>();

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;

    @Override
    public DnBWhiteCache lookupWhiteCache(MatchKeyTuple matchKeyTuple, String dataCloudVersion) {
        DnBWhiteCache input = new DnBWhiteCache(matchKeyTuple);
        DnBWhiteCache output = getWhiteCacheMgr(dataCloudVersion).findByKey(input);
        if (output != null) {
            output.parseCacheContext();
        }
        return output;
    }

    @Override
    public Map<String, DnBWhiteCache> batchLookupWhiteCache(Map<String, MatchKeyTuple> matchKeyTuples,
            String dataCloudVersion) {
        List<String> keys = new ArrayList<String>();
        List<String> lookupRequestIds = new ArrayList<String>();
        for (String lookupRequestId : matchKeyTuples.keySet()) {
            DnBWhiteCache input = new DnBWhiteCache(matchKeyTuples.get(lookupRequestId));
            keys.add(input.getId());
            lookupRequestIds.add(lookupRequestId);
        }
        List<DnBWhiteCache> outputs = getWhiteCacheMgr(dataCloudVersion).batchFindByKey(keys);
        Map<String, DnBWhiteCache> result = new HashMap<String, DnBWhiteCache>();
        for (int i = 0; i < outputs.size(); i++) {
            DnBWhiteCache output = outputs.get(i);
            if (output != null) {
                output.parseCacheContext();
            }
            result.put(lookupRequestIds.get(i), output);
        }
        return result;
    }

    @Override
    public DnBWhiteCache addWhiteCache(DnBMatchContext context, String dataCloudVersion) {
        DnBWhiteCache cache = new DnBWhiteCache(
                context.getInputNameLocation() != null ? context.getInputNameLocation() : new NameLocation(),
                context.getInputEmail(), context.getDuns(), context.getConfidenceCode(), context.getMatchGrade());
        getWhiteCacheMgr(dataCloudVersion).create(cache);
        return cache;
    }

    @Override
    public List<DnBWhiteCache> batchAddWhiteCache(List<DnBMatchContext> contexts, String dataCloudVersion) {
        List<DnBWhiteCache> caches = new ArrayList<DnBWhiteCache>();
        for(DnBMatchContext context : contexts) {
            DnBWhiteCache cache = new DnBWhiteCache(
                    context.getInputNameLocation() != null ? context.getInputNameLocation() : new NameLocation(),
                    context.getInputEmail(), context.getDuns(), context.getConfidenceCode(), context.getMatchGrade());
            caches.add(cache);
        }
        getWhiteCacheMgr(dataCloudVersion).batchCreate(caches);
        return caches;
    }

    public DnBWhiteCacheEntityMgr getWhiteCacheMgr(String version) {
        DnBWhiteCacheEntityMgr whiteCacheEntityMgr = whiteCacheEntityMgrs.get(version);
        if (whiteCacheEntityMgr == null)
            whiteCacheEntityMgr = getWhiteCacheMgrSync(version);
        return whiteCacheEntityMgr;
    }

    private synchronized DnBWhiteCacheEntityMgr getWhiteCacheMgrSync(String version) {
        DnBWhiteCacheEntityMgr whiteCacheEntityMgr = whiteCacheEntityMgrs.get(version);

        if (whiteCacheEntityMgr == null) {
            DataCloudVersion dataCloudVersion = versionEntityMgr.findVersion(version);
            if (dataCloudVersion == null) {
                throw new IllegalArgumentException("Cannot find the specified data cloud version " + version);
            }
            log.info("Use " + version + " as full version of DnBWhiteCache for " + version);
            whiteCacheEntityMgr = new DnBWhiteCacheEntityMgrImpl(messageService, dataService, version);
            whiteCacheEntityMgr.init();
            whiteCacheEntityMgrs.put(version, whiteCacheEntityMgr);
        }

        return whiteCacheEntityMgr;
    }
}
