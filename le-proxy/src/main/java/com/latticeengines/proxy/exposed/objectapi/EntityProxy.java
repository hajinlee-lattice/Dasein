package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.CDLConsolidate;
import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("entityProxy")
public class EntityProxy extends MicroserviceRestApiProxy {

    private final WatcherCache<String, Long> countCache;

    private final WatcherCache<String, DataPage> dataCache;

    private final WatcherCache<String, Map<String, Long>> ratingCache;

    @SuppressWarnings("unchecked")
    public EntityProxy() {
        super("objectapi/customerspaces");
        countCache = WatcherCache.builder() //
                .name("EntityCountCache") //
                .watch(CDLConsolidate) //
                .maximum(2000) //
                .load(o -> {
                    String serializedKey = (String) o;
                    return getCountFromObjectApi(serializedKey);
                }) //
                .build();
        dataCache = WatcherCache.builder() //
                .name("EntityDataCache") //
                .watch(CDLConsolidate) //
                .maximum(200) //
                .load(o -> {
                    String serializedKey = (String) o;
                    return getDataFromObjectApi(serializedKey);
                }) //
                .build();
        ratingCache = WatcherCache.builder() //
                .name("EntityDataCache") //
                .watch(CDLConsolidate) //
                .maximum(2000) //
                .load(o -> {
                    String serializedKey = (String) o;
                    return getRatingCountFromObjectApi(serializedKey);
                }) //
                .build();

        countCache.setEvictKeyResolver((updateSignal, existingKeys) -> {
            String customerSpace = shortenCustomerSpace(updateSignal);
            List<String> keysToEvict = new ArrayList<>();
            existingKeys.forEach(key -> {
                String tenantId = key.substring(0, key.indexOf("|"));
                if (customerSpace.equals(tenantId)) {
                    keysToEvict.add(key);
                }
            });
            return keysToEvict;
        });
        countCache.setRefreshKeyResolver((updateSignal, existingKeys) -> Collections.emptyList());
        dataCache.setEvictKeyResolver((updateSignal, existingKeys) -> {
            String customerSpace = shortenCustomerSpace(updateSignal);
            List<String> keysToEvict = new ArrayList<>();
            existingKeys.forEach(key -> {
                String tenantId = key.substring(0, key.indexOf("|"));
                if (customerSpace.equals(tenantId)) {
                    keysToEvict.add(key);
                }
            });
            return keysToEvict;
        });
        dataCache.setRefreshKeyResolver((updateSignal, existingKeys) -> Collections.emptyList());
        ratingCache.setEvictKeyResolver((updateSignal, existingKeys) -> {
            String customerSpace = shortenCustomerSpace(updateSignal);
            List<String> keysToEvict = new ArrayList<>();
            existingKeys.forEach(key -> {
                String tenantId = key.substring(0, key.indexOf("|"));
                if (customerSpace.equals(tenantId)) {
                    keysToEvict.add(key);
                }
            });
            return keysToEvict;
        });
        ratingCache.setRefreshKeyResolver((updateSignal, existingKeys) -> Collections.emptyList());
    }

    public long getCount(String customerSpace, FrontEndQuery frontEndQuery) {
        return countCache
                .get(String.format("%s|%s", shortenCustomerSpace(customerSpace), JsonUtils.serialize(frontEndQuery)));
    }

    public DataPage getData(String customerSpace, FrontEndQuery frontEndQuery) {
        return dataCache
                .get(String.format("%s|%s", shortenCustomerSpace(customerSpace), JsonUtils.serialize(frontEndQuery)));
    }

    @SuppressWarnings({ "rawtypes" })
    public Map<String, Long> getRatingCount(String customerSpace, FrontEndQuery frontEndQuery) {
        return ratingCache
                .get(String.format("%s|%s", shortenCustomerSpace(customerSpace), JsonUtils.serialize(frontEndQuery)));
    }

    private long getCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/count", tenantId);
        return post("getCount", url, frontEndQuery, Long.class);
    }

    private DataPage getDataFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/data", tenantId);
        return post("getData", url, frontEndQuery, DataPage.class);
    }

    @SuppressWarnings({ "rawtypes" })
    private Map<String, Long> getRatingCountFromObjectApi(String serializedKey) {
        String tenantId = serializedKey.substring(0, serializedKey.indexOf("|"));
        String serializedQuery = serializedKey.substring(tenantId.length() + 1);
        FrontEndQuery frontEndQuery = JsonUtils.deserialize(serializedQuery, FrontEndQuery.class);
        String url = constructUrl("/{customerSpace}/entity/ratingcount", tenantId);
        Map map = post("getRatingCount", url, frontEndQuery, Map.class);
        if (map == null) {
            return null;
        } else {
            return JsonUtils.convertMap(map, String.class, Long.class);
        }
    }
}
