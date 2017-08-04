package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("entityProxy")
public class EntityProxy extends MicroserviceRestApiProxy {
    public EntityProxy() {
        super("objectapi/customerspaces");
    }

    public long getCount(String customerSpace, BusinessEntity entity, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/entities/{entity}/count", shortenCustomerSpace(customerSpace),
                entity);
        return post("getCount", url, frontEndQuery, Long.class);
    }

    public DataPage getData(String customerSpace, BusinessEntity entity, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/entities/{entity}/data", shortenCustomerSpace(customerSpace),
                entity);
        return post("getData", url, frontEndQuery, DataPage.class);
    }
}
