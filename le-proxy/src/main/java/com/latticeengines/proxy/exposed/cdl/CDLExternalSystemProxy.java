package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("cdlExternalSystemProxy")
public class CDLExternalSystemProxy extends MicroserviceRestApiProxy {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/cdlexternalsystem";

    protected CDLExternalSystemProxy() {
        super("cdl");
    }

    public CDLExternalSystem getCDLExternalSystem(String customerSpace) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        return get("get CDL external system", url, CDLExternalSystem.class);
    }

    public void createOrUpdateCDLExternalSystem(String customerSpace, CDLExternalSystem cdlExternalSystem) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        post("create or update a CDL external system", url, cdlExternalSystem, Void.class);
    }

}
