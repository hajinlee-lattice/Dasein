package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("ratingEngineDashboardProxy")
public class RatingEngineDashboardProxy extends MicroserviceRestApiProxy {
    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/ratingengines";

    protected RatingEngineDashboardProxy() {
        super("cdl");
    }

    public RatingEngineDashboard getRatingEngineDashboardById(String customerSpace, String ratingEngineId) {
        String url = constructUrl(URL_PREFIX + "/{ratingEngineId}/dashboard", shortenCustomerSpace(customerSpace),
                ratingEngineId);
        return get("getRatingEngineDashboardById", url, RatingEngineDashboard.class);
    }
}
