package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.AggregationHistory;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.AggregationHistoryProxy;

@Component("aggregationHistoryProxy")
public class AggregationHistoryProxyImpl extends MicroserviceRestApiProxy implements AggregationHistoryProxy {

    protected AggregationHistoryProxyImpl() {
        super("cdl");
    }

    @Override
    public AggregationHistory create(String customerSpace, AggregationHistory aggregationHistory) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("customerspaces/{customerSpace}/aggregation-history",
                shortenCustomerSpace(customerSpace)));
        return post("create aggregation history ", url.toString(), aggregationHistory, AggregationHistory.class);
    }
}
