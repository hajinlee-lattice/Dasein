package com.latticeengines.proxy.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.time.Duration;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;

import reactor.core.publisher.Mono;


@Component("eventProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class EventProxyImpl extends MicroserviceRestApiProxy implements EventProxy {

    private final EventProxyImpl _eventProxy;

    public EventProxyImpl(EventProxyImpl eventProxy) {
        super("objectapi/customerspaces");
        this._eventProxy = eventProxy;
    }

    @Override
    public Long getScoringCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return Long.valueOf(_eventProxy.getScoringCountFromCache(shortenCustomerSpace(customerSpace), frontEndQuery));
    }

    @Override
    public Long getTrainingCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return Long.valueOf(_eventProxy.getTrainingCountFromCache(shortenCustomerSpace(customerSpace), frontEndQuery));
    }

    @Override
    public Long getEventCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return Long.valueOf(_eventProxy.getEventCountFromCache(shortenCustomerSpace(customerSpace), frontEndQuery));
    }

    @Override
    public Long getScoringCount(String customerSpace, EventFrontEndQuery frontEndQuery,
                         DataCollection.Version version) {
        return getScoringCountFromObjectApi(customerSpace, frontEndQuery, version).block(Duration.ofHours(1));
    }

    @Override
    public Long getTrainingCount(String customerSpace, EventFrontEndQuery frontEndQuery,
                          DataCollection.Version version) {
        return getTrainingCountFromObjectApi(customerSpace, frontEndQuery, version).block(Duration.ofHours(1));
    }

    @Override
    public Long getEventCount(String customerSpace, EventFrontEndQuery frontEndQuery,
                       DataCollection.Version version) {
        return getEventCountFromObjectApi(customerSpace, frontEndQuery, version).block(Duration.ofHours(1));
    }

    @Override
    public DataPage getScoringTuples(String customerSpace, EventFrontEndQuery frontEndQuery,
                                     DataCollection.Version version) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            DataPage dataPage = getScoringTuplesNonBlocking(customerSpace, frontEndQuery, version)
                    .block(Duration.ofHours(1));
            int count = dataPage == null ? 0 : dataPage.getData().size();
            String msg = "Fetched a page of " + count + " scoring tuples.";
            timer.setTimerMessage(msg);
            return dataPage;
        }
    }

    @Override
    public DataPage getTrainingTuples(String customerSpace, EventFrontEndQuery frontEndQuery,
                                      DataCollection.Version version) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            DataPage dataPage = getTrainingTuplesNonBlocking(customerSpace, frontEndQuery, version)
                    .block(Duration.ofHours(1));
            int count = dataPage == null ? 0 : dataPage.getData().size();
            String msg = "Fetched a page of " + count + " training tuples.";
            timer.setTimerMessage(msg);
            return dataPage;
        }
    }

    @Override
    public DataPage getEventTuples(String customerSpace, EventFrontEndQuery frontEndQuery,
                                   DataCollection.Version version) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            DataPage dataPage = getEventTuplesNonBlocking(customerSpace, frontEndQuery, version)
                    .block(Duration.ofHours(1));
            int count = dataPage == null ? 0 : dataPage.getData().size();
            String msg = "Fetched a page of " + count + " event tuples.";
            timer.setTimerMessage(msg);
            return dataPage;
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|event_scoring_count\", #tenantId, #frontEndQuery)")
    public String getScoringCountFromCache(String tenantId, EventFrontEndQuery frontEndQuery) {
        return String.valueOf( //
                getScoringCountFromObjectApi(tenantId, frontEndQuery, null).block(Duration.ofHours(1)));
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|event_training_count\", #tenantId, #frontEndQuery)")
    public String getTrainingCountFromCache(String tenantId, EventFrontEndQuery frontEndQuery) {
        return String.valueOf( //
                getTrainingCountFromObjectApi(tenantId, frontEndQuery, null).block(Duration.ofHours(1)));
    }

    @Cacheable(cacheNames = CacheName.Constants.ObjectApiCacheName, key = "T(java.lang.String).format(\"%s|%s|event_event_count\", #tenantId, #frontEndQuery)")
    public String getEventCountFromCache(String tenantId, EventFrontEndQuery frontEndQuery) {
        return String.valueOf( //
                getEventCountFromObjectApi(tenantId, frontEndQuery, null).block(Duration.ofHours(1)));
    }

    private Mono<Long> getScoringCountFromObjectApi(String customerSpace, EventFrontEndQuery frontEndQuery,
                                       DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/count/scoring",
                shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return postMono("getScoringCount", url, frontEndQuery, Long.class);
    }

    private Mono<Long> getTrainingCountFromObjectApi(String customerSpace, EventFrontEndQuery frontEndQuery,
                                        DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/count/training",
                shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return postMono("getTrainingCount", url, frontEndQuery, Long.class);
    }

    private Mono<Long> getEventCountFromObjectApi(String customerSpace, EventFrontEndQuery frontEndQuery,
                                     DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/count/event",
                shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return postMono("getEventCount", url, frontEndQuery, Long.class);
    }

    private Mono<DataPage> getScoringTuplesNonBlocking(String customerSpace,
                                                       EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/data/scoring",
                shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return postMonoKryo("getScoringTuples", url, frontEndQuery, DataPage.class);
    }

    private Mono<DataPage> getTrainingTuplesNonBlocking(String customerSpace,
                                                        EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/data/training",
                shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return postMonoKryo("getTrainingTuples", url, frontEndQuery, DataPage.class);
    }

    private Mono<DataPage> getEventTuplesNonBlocking(String customerSpace,
                                                     EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/data/event",
                shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return postMonoKryo("getEventTuples", url, frontEndQuery, DataPage.class);
    }

    private String appendDataCollectionVersion(String url, DataCollection.Version version) {
        if (version == null) {
            return url;
        } else {
            return url + "?version=" + version;
        }
    }

}
