package com.latticeengines.proxy.exposed.objectapi;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.network.exposed.objectapi.AccountInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("accountProxy")
public class AccountProxy extends MicroserviceRestApiProxy implements AccountInterface {
    public AccountProxy() {
        super("objectapi/customerspaces");
    }

    @Override
    public long getCount(String customerSpace, Query query) {
        String url = constructUrl("/{customerSpace}/accounts/count", customerSpace);
        return post("getCount", url, query, Long.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataPage getData(String customerSpace, Query query) {
        String url = constructUrl("/{customerSpace}/accounts/data", customerSpace);
        return post("getData", url, query, DataPage.class);
    }
}
