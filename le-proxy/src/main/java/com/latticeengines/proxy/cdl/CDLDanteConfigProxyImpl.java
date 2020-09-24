package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.CDLDanteConfigProxy;

@Component("cdlDanteConfigProxy")
public class CDLDanteConfigProxyImpl extends MicroserviceRestApiProxy implements CDLDanteConfigProxy {

    private static final Logger log = LoggerFactory.getLogger(CDLDanteConfigProxyImpl.class);

    protected CDLDanteConfigProxyImpl() {
        super("cdl");
    }

    @Override
    public DanteConfigurationDocument getDanteConfiguration(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/dante-configuration",
                shortenCustomerSpace(customerSpace));
        return get("get generated danta config response", url, DanteConfigurationDocument.class);
    }

    @Override
    public void refreshDanteConfiguration(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/dante-configuration/refresh",
                shortenCustomerSpace(customerSpace));
        CompletableFuture
                .runAsync(
                        () -> post("get generated dante config response", url, null, DanteConfigurationDocument.class))
                .whenComplete((task, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to refresh dante configuration document for tenant:"
                                + shortenCustomerSpace(customerSpace));
                    } else {
                        log.info("Successfully refreshed dante configuration document for tenant:"
                                + shortenCustomerSpace(customerSpace));
                    }
                });
    }
}
