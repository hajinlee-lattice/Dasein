package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("cdlMaintenanceProxy")
public class CDLMaintenanceProxy extends MicroserviceRestApiProxy {

    protected CDLMaintenanceProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public Boolean maintenance(String customerSpace, MaintenanceOperationConfiguration configuration) {
        String url = constructUrl("/customerspaces/{customerSpace}/maintenance",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<Boolean> doc = post("cdlmaintenance", url, configuration, ResponseDocument.class);
        if (doc == null) {
            return false;
        }
        return doc.getResult();
    }
}
