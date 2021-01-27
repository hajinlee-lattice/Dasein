package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.integration.BrokerReference;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.InboundConnectionProxy;

@Component("inboundConnectionProxy")
public class InboundConnectionProxyImpl extends MicroserviceRestApiProxy implements InboundConnectionProxy {

    protected InboundConnectionProxyImpl() {
        super("cdl");
    }

    @Override
    public BrokerReference getBrokerReference(String customerSpace, BrokerReference brokerReference) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("customerspaces/{customerSpace}/inboundconnection/brokerreference",
                shortenCustomerSpace(customerSpace)));
        return post("get broker reference", url.toString(), brokerReference, BrokerReference.class);
    }

    @Override
    public BrokerReference updateBroker(String customerSpace, BrokerReference brokerReference) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("customerspaces/{customerSpace}/inboundconnection", shortenCustomerSpace(customerSpace)));
        return post("update broker", url.toString(), brokerReference, BrokerReference.class);
    }
}
