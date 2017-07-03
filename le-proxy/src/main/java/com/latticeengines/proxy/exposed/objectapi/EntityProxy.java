package com.latticeengines.proxy.exposed.objectapi;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.network.exposed.objectapi.EntityInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("entityProxy")
public class EntityProxy extends MicroserviceRestApiProxy implements EntityInterface {
    public EntityProxy() {
        super("objectapi/customerspaces");
    }

    @Override
    public long getCount(String customerSpace, Query query) {
        String url = constructUrl("/{customerSpace}/entities/count", customerSpace);
        return post("getCount", url, query, Long.class);
    }

    @Override
    public DataPage getData(String customerSpace, Query query) {
        String url = constructUrl("/{customerSpace}/entities/data", customerSpace);
        return post("getData", url, query, DataPage.class);
    }
}
