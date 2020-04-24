package com.latticeengines.proxy.objectapi;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.objectapi.ActivityProxy;

@Component("activityProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class ActivityProxyImpl extends MicroserviceRestApiProxy implements ActivityProxy {

    public ActivityProxyImpl() {
        super("objectapi/customerspaces");
    }

    @Override
    public DataPage getData(String customerSpace, DataCollection.Version version,
            ActivityTimelineQuery activityTimelineQuery) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            String url = constructUrl("/{customerSpace}/activity/data", customerSpace);

            if (version != null) {
                url += "&version={version}";
            }
            return post("get-activity-data", url, activityTimelineQuery, DataPage.class);
        }
    }
}
