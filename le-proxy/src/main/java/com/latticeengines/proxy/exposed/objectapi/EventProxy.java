package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("eventProxy")
public class EventProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    public EventProxy() {
        super("objectapi/customerspaces");
    }

    public Long getScoringCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getScoringCount(customerSpace, frontEndQuery, null);
    }

    public Long getTrainingCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getTrainingCount(customerSpace, frontEndQuery, null);
    }

    public Long getEventCount(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getEventCount(customerSpace, frontEndQuery, null);
    }

    public DataPage getScoringTuples(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getScoringTuples(customerSpace, frontEndQuery, null);
    }

    public DataPage getTrainingTuples(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getTrainingTuples(customerSpace, frontEndQuery, null);
    }

    public DataPage getEventTuples(String customerSpace, EventFrontEndQuery frontEndQuery) {
        return getEventTuples(customerSpace, frontEndQuery, null);
    }

    public Long getScoringCount(String customerSpace, EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/count/scoring", shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return post("getScoringCount", url, frontEndQuery, Long.class);
    }

    public Long getTrainingCount(String customerSpace, EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/count/training", shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return post("getTrainingCount", url, frontEndQuery, Long.class);
    }

    public Long getEventCount(String customerSpace, EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/count/event", shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return post("getEventCount", url, frontEndQuery, Long.class);
    }

    public DataPage getScoringTuples(String customerSpace, EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/data/scoring", shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return post("getScoringTuples", url, frontEndQuery, DataPage.class);
    }

    public DataPage getTrainingTuples(String customerSpace, EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/data/training", shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return post("getTrainingTuples", url, frontEndQuery, DataPage.class);
    }

    public DataPage getEventTuples(String customerSpace, EventFrontEndQuery frontEndQuery, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/event/data/event", shortenCustomerSpace(customerSpace));
        url = appendDataCollectionVersion(url, version);
        RestrictionOptimizer.optimize(frontEndQuery);
        return post("getEventTuples", url, frontEndQuery, DataPage.class);
    }

    private String appendDataCollectionVersion(String url, DataCollection.Version version) {
        if (version == null) {
            return url;
        } else {
            return url + "?version=" + version;
        }
    }

}
