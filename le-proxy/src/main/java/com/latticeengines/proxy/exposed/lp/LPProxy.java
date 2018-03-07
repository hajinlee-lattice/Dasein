package com.latticeengines.proxy.exposed.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.serviceapps.lp.LPBootstrapRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;


@Component("lpProxy")
public class LPProxy extends MicroserviceRestApiProxy {

    protected LPProxy() {
        super("lp");
    }

    public void bootstrap(LPBootstrapRequest bootstrapRequest) {
        String url = constructUrl("/tenant/");
        post("bootstrap lp tenant", url, bootstrapRequest, ResponseDocument.class);
    }

    public void cleanup(String tenantId) {
        String url = constructUrl("/tenant/{tenantId}/", shortenCustomerSpace(tenantId));
        delete("cleanup lp tenant", url);
    }
}
