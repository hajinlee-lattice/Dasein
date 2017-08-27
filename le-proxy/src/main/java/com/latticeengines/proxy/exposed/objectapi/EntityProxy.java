package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("entityProxy")
public class EntityProxy extends MicroserviceRestApiProxy {
    public EntityProxy() {
        super("objectapi/customerspaces");
    }

    public long getCount(String customerSpace, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/entity/count", shortenCustomerSpace(customerSpace));
        return post("getCount", url, frontEndQuery, Long.class);
    }

    public DataPage getData(String customerSpace, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/entity/data", shortenCustomerSpace(customerSpace));
        return post("getData", url, frontEndQuery, DataPage.class);
    }

    @SuppressWarnings({ "rawtypes" })
    public Map<String, Long> getRatingCount(String customerSpace, FrontEndQuery frontEndQuery) {
        String url = constructUrl("/{customerSpace}/entity/ratingcount", shortenCustomerSpace(customerSpace));
        Map map = post("getRatingCount", url, frontEndQuery, Map.class);
        if (map == null) {
            return null;
        } else {
            return JsonUtils.convertMap(map, String.class, Long.class);
        }
    }
}
