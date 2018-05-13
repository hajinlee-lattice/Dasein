package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataCollectionStatusProxy")
public class DataCollectionStatusProxy extends MicroserviceRestApiProxy {

    protected DataCollectionStatusProxy() {
        super("cdl");
    }

    public DataCollectionStatusDetail getOrCreateDataCollectionStatus(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/status",
                shortenCustomerSpace(customerSpace));
        return get("get dataCollection status", url, DataCollectionStatusDetail.class);
    }

    public void saveOrUpdateDataCollectionStatus(String customerSpace, DataCollectionStatusDetail detail) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/status",
                shortenCustomerSpace(customerSpace));
        post("saveOrUpdateStatus", url, detail, Void.class);
    }

}
