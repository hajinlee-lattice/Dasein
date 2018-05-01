package com.latticeengines.proxy.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;


@Component("dataUnitProxy")
public class DataUnitProxyImpl extends MicroserviceRestApiProxy implements DataUnitProxy {

    protected DataUnitProxyImpl() {
        super("metadata");
    }

    @Override
    public DataUnit create(String customerSpace, DataUnit dataUnit) {
        String url = constructUrl("/customerspaces/{customerSpace}/dataunit", shortenCustomerSpace(customerSpace));
        return post("create data unit", url, dataUnit, DataUnit.class);
    }
}
