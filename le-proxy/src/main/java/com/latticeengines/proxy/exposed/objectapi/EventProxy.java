package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.time.Duration;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

import reactor.core.publisher.Mono;

@Component("eventProxy")
public class EventProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    public EventProxy() {
        super("objectapi/customerspaces");
    }

    public Long getScoringCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getScoringCount(customerSpace, frontEndQuery, null).block(Duration.ofHours(1));
    }

    public Long getTrainingCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getTrainingCount(customerSpace, frontEndQuery, null).block(Duration.ofHours(1));
    }

    public Long getEventCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getEventCount(customerSpace, frontEndQuery, null).block(Duration.ofHours(1));
    }

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

    private Mono<Long> getScoringCount(String customerSpace, EventFrontEndQuery frontEndQuery,
            DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/count/scoring",
                shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return postMono("getScoringCount", url, frontEndQuery, Long.class);
    }

    private Mono<Long> getTrainingCount(String customerSpace, EventFrontEndQuery frontEndQuery,
            DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/count/training",
                shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return postMono("getTrainingCount", url, frontEndQuery, Long.class);
    }

    private Mono<Long> getEventCount(String customerSpace, EventFrontEndQuery frontEndQuery,
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
