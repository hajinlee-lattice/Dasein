package com.latticeengines.proxy.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.dcp.AppendConfigProxy;

@Component("appendConfigProxy")
public class AppendConfigProxyImpl extends MicroserviceRestApiProxy implements AppendConfigProxy {

    protected AppendConfigProxyImpl() {
        super("dcp");
    }

    @Override
    public DataBlockEntitlementContainer getEntitlement(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/append-config/entitlement", //
                shortenCustomerSpace(customerSpace));
        return get("get entitlement", url, DataBlockEntitlementContainer.class);
    }

}
