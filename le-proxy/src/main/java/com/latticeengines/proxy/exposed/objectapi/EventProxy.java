package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.network.exposed.objectapi.EventInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("eventProxy")
public class EventProxy extends MicroserviceRestApiProxy implements EventInterface {

    public EventProxy() {
        super("objectapi/customerspaces");
    }

    @Override
    public DataPage getScoringTuples(String customerSpace, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/event/data/scoring", shortenCustomerSpace(customerSpace));
        return post("getScoringTuples", url, frontEndQuery, DataPage.class);
    }

    @Override
    public DataPage getTrainingTuples(String customerSpace, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/event/data/training", shortenCustomerSpace(customerSpace));
        return post("getTrainingTuples", url, frontEndQuery, DataPage.class);
    }

    @Override
    public DataPage getEventTuples(String customerSpace, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/event/data/event", shortenCustomerSpace(customerSpace));
        return post("getEventTuples", url, frontEndQuery, DataPage.class);
    }

}
