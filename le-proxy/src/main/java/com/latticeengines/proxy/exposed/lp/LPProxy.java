package com.latticeengines.proxy.exposed.lp;


import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("lpProxy")
public class LPProxy extends MicroserviceRestApiProxy {

    protected LPProxy() {
        super("lp");
    }

    public AttrConfigRequest getAttrConfigRequest(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerspace}/attrconfig", shortenCustomerSpace(customerSpace));
        return get("getAttrConfigRequest", url, AttrConfigRequest.class);
    }
}
