package com.latticeengines.proxy.exposed.cdl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("dataIntegrationMonitoringProxy")
public class DataIntegrationMonitoringProxy extends MicroserviceRestApiProxy implements ProxyInterface{

    private static final String URL_PREFIX = "/dataintegration";

    public DataIntegrationMonitoringProxy() {
        super("cdl");
    }

    public Boolean createOrUpdateStatus(DataIntegrationStatusMonitorMessage message) {
        String url = constructUrl(URL_PREFIX);
        return post("create or update data integration status", url, message, Boolean.class);
    }
}
