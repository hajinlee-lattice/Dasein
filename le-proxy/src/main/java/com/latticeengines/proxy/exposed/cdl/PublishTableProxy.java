package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.elasticsearch.PublishTableToESRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("publishTableProxy")
public class PublishTableProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(PublishTableProxy.class);

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/publish";

    protected PublishTableProxy() {
        super("cdl");
    }


    public String publishTableToES(@NotNull String customerSpace, PublishTableToESRequest request) {
        log.info("publish table to es for {} with request {}", customerSpace, JsonUtils.serialize(request));
        String url = constructUrl(URL_PREFIX + "/es/tables", shortenCustomerSpace(customerSpace));
        return post("publishTableToES", url, request, String.class);
    }
}
