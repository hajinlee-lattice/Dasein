package com.latticeengines.proxy.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.dcp.EntitlementProxy;

@Component("entitlementProxy")
public class EntitlementProxyImpl extends MicroserviceRestApiProxy implements EntitlementProxy {

    private static final Logger log = LoggerFactory.getLogger(EntitlementProxyImpl.class);

    protected EntitlementProxyImpl() {
        super("dcp");
    }

    @Override
    public DataBlockEntitlementContainer getEntitlement(String customerSpace, String domainName, String recordType) {
        String url = constructUrl("/customerspaces/{customerSpace}/entitlement/{domainName}/{recordType}", //
                shortenCustomerSpace(customerSpace), domainName, recordType);
        return get("get entitlement", url, DataBlockEntitlementContainer.class);
    }

}
