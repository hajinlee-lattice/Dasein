package com.latticeengines.domain.exposed.ratings.coverage;

import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class ProductAndEventQueryPair {

    private String productId;
    private EventFrontEndQuery eventFrontEndQuery;

    public ProductAndEventQueryPair(String productId, EventFrontEndQuery eventFrontEndQuery) {
        this.productId = productId;
        this.eventFrontEndQuery = eventFrontEndQuery;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public EventFrontEndQuery getEventFrontEndQuery() {
        return eventFrontEndQuery;
    }

    public void setEventFrontEndQuery(EventFrontEndQuery eventFrontEndQuery) {
        this.eventFrontEndQuery = eventFrontEndQuery;
    }
}
