package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.ActivityMetricsWithAction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("activityMetricsProxy")
public class ActivityMetricsProxy extends MicroserviceRestApiProxy {

    protected ActivityMetricsProxy() {
        super("cdl");
    }

    public List<ActivityMetrics> getActivityMetrics(String customerSpace, ActivityType type) {
        String url = constructUrl("/customerspaces/{customerSpace}/metrics/{type}", shortenCustomerSpace(customerSpace),
                type.name());
        List<?> list = get("Get all the metrics for specific activity type", url, List.class);
        return JsonUtils.convertList(list, ActivityMetrics.class);
    }

    public List<ActivityMetrics> getActiveActivityMetrics(String customerSpace, ActivityType type) {
        String url = constructUrl("/customerspaces/{customerSpace}/metrics/{type}/active",
                shortenCustomerSpace(customerSpace), type.name());
        List<?> list = get("Get all the active metrics for specific activity type", url, List.class);
        return JsonUtils.convertList(list, ActivityMetrics.class);
    }

    public ActivityMetricsWithAction save(String customerSpace, ActivityType type, List<ActivityMetrics> metrics) {
        String url = constructUrl("/customerspaces/{customerSpace}/metrics/{type}", shortenCustomerSpace(customerSpace),
                type.name());
        return post("Save metrics for specific activity type", url, metrics, ActivityMetricsWithAction.class);
    }

}
