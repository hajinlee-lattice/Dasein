package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

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
        String url = constructUrl("/{customerSpace}/entities/count", shortenCustomerSpace(customerSpace));
        return post("getCount", url, query, Long.class);
    }

    @Override
    public DataPage getData(String customerSpace, Query query) {
        String url = constructUrl("/{customerSpace}/entities/data", shortenCustomerSpace(customerSpace));
        return post("getData", url, query, DataPage.class);
    }
}
