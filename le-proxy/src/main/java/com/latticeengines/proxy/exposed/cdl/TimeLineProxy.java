package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
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

    public List<TimeLine> findAll(String customerSpace) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        List<?> list = get("findByEntity", url, List.class);
        return JsonUtils.convertList(list, TimeLine.class);
    }

    public TimeLine findByEntity(String customerSpace, BusinessEntity entity) {
        String url = constructUrl(URL_PREFIX + "/entity/" + entity, shortenCustomerSpace(customerSpace));
        return get("findByEntity", url, TimeLine.class);
    }

    public void createTimeline(String customerSpace, TimeLine timeLine) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        post("create timeline", url, timeLine);
    }

    public void createDefaultTimeLine(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/createDefault", shortenCustomerSpace(customerSpace));
        post("create default timeline", url, null);
    }

}
