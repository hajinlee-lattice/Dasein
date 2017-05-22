package com.latticeengines.proxy.exposed.metadata;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.network.exposed.metadata.DataCollectionInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataCollectionProxy")
public class DataCollectionProxy extends MicroserviceRestApiProxy implements DataCollectionInterface {
    private static final Log log = LogFactory.getLog(DataCollectionProxy.class);

    private LoadingCache<Pair<String, DataCollectionType>, DataCollection> dataCollectionCache;

    private ScheduledExecutorService cacheReloader = Executors.newSingleThreadScheduledExecutor();

    protected DataCollectionProxy() {
        super("metadata");

        dataCollectionCache = CacheBuilder.newBuilder().maximumSize(1000).expireAfterWrite(60, TimeUnit.MINUTES)
                .build(new CacheLoader<Pair<String, DataCollectionType>, DataCollection>() {
                    @Override
                    public DataCollection load(Pair<String, DataCollectionType> key) throws Exception {
                        String customerSpace = key.getLeft();
                        DataCollectionType type = key.getRight();
                        log.info(String.format("Loading collection with type %s for customer %s...", type,
                                customerSpace));
                        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/types/{type}",
                                customerSpace, type);
                        return get("getDataCollection", url, DataCollection.class);
                    }
                });
        cacheReloader.scheduleWithFixedDelay(() -> {
            for (Pair<String, DataCollectionType> pair : dataCollectionCache.asMap().keySet()) {
                dataCollectionCache.refresh(pair);
            }
        }, 0, 15, TimeUnit.MINUTES);

    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DataCollection> getDataCollections(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections", customerSpace);
        List list = get("getDataCollections", url, List.class);
        return JsonUtils.convertList(list, DataCollection.class);
    }

    @Override
    public DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type) {
        Pair<String, DataCollectionType> key = new ImmutablePair<>(customerSpace, type);

        try {
            return dataCollectionCache.get(key);
        } catch (ExecutionException e) {
            log.error(
                    String.format(
                            "Failed to retrieve DataCollection of type %s for customer %s from cache.  Falling back to requesting it explicitly",
                            type, customerSpace), e);
        }

        String url = constructUrl("/customerspaces/{customerSpace}/datacollections/types/{type}", customerSpace, type);
        return get("getDataCollection", url, DataCollection.class);
    }

    @Override
    public DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollections", customerSpace);
        return post("createOrUpdateDataCollection", url, dataCollection, DataCollection.class);
    }
}
