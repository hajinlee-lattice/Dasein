package com.latticeengines.proxy.exposed.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("dcpProxy")
public class DCPProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected DCPProxy() {
        super("dcp");
    }

    public DCPProxy(String hostPort) {
        super(hostPort, "dcp");
    }

    public ApplicationId startImport(String customerSpace, DCPImportRequest request) {
        String baseUrl = "/customerspaces/{customerSpace}/uploads/startimport";
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace));
        String appIdStr = post("Start DCP import", url, request, String.class);
        return ApplicationId.fromString(appIdStr);
    }

}
