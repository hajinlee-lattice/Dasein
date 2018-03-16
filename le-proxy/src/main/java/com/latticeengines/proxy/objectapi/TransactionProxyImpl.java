package com.latticeengines.proxy.objectapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyUtils;
import com.latticeengines.proxy.exposed.objectapi.TransactionProxy;

@Component("transactionProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class TransactionProxyImpl extends MicroserviceRestApiProxy implements TransactionProxy {

    public TransactionProxyImpl() {
        super("/objectapi");
    }

    private static final Logger log = LoggerFactory.getLogger(TransactionProxyImpl.class);

    public String getMaxTransactionDate(String customerSpace, DataCollection.Version version) {
        String url = constructUrl("/customerspaces/{customerSpace}/transactions/maxtransactiondate?version={version}",
                ProxyUtils.shortenCustomerSpace(customerSpace), version);
        return get("getMaxTransactionDate", url, String.class);
    }
}
