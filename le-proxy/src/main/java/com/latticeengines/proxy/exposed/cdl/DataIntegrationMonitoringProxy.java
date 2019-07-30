package com.latticeengines.proxy.exposed.cdl;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("dataIntegrationMonitoringProxy")
public class DataIntegrationMonitoringProxy extends MicroserviceRestApiProxy implements ProxyInterface{

    private static final String URL_PREFIX = "/dataintegration";

    public DataIntegrationMonitoringProxy() {
        super("cdl");
    }

    public Map<String, Boolean> createOrUpdateStatus(List<DataIntegrationStatusMonitorMessage> messages) {
        String url = constructUrl(URL_PREFIX);
        return JsonUtils.convertMap(post("create or update data integration status", url, messages, Map.class),
                String.class, Boolean.class);
    }
}
