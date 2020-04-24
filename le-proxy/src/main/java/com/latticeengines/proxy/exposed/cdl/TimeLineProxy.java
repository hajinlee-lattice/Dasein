package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("timeLineProxy")
public class TimeLineProxy extends MicroserviceRestApiProxy implements ProxyInterface {
    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/timelines";

    public TimeLineProxy() {
        super("cdl");
    }

    public TimeLine findByEntity(String customerSpace, BusinessEntity entity) {
        String url = constructUrl(URL_PREFIX + "/entity/" + entity, shortenCustomerSpace(customerSpace));
        return get("findByEntity", url, TimeLine.class);
    }

    public void createTimeline(String customerSpace, TimeLine timeLine) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        post("create timeline", url, timeLine);
    }

}
