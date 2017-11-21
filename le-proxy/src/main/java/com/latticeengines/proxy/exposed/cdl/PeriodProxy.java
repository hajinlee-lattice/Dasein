package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Service("periodProxy")
public class PeriodProxy extends MicroserviceRestApiProxy {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/periods";

    protected PeriodProxy() {
        super("cdl");
    }

    public List<String> getPeriodNames(@PathVariable String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/names", shortenCustomerSpace(customerSpace));
        List list = get("get period names", url, List.class);
        return JsonUtils.convertList(list, String.class);
    }


}
