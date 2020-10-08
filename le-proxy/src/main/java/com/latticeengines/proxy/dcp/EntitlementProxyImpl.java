package com.latticeengines.proxy.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.dcp.EntitlementProxy;

@Component("entitlementProxy")
public class EntitlementProxyImpl extends MicroserviceRestApiProxy implements EntitlementProxy {

    private static final Logger log = LoggerFactory.getLogger(EntitlementProxyImpl.class);

    protected EntitlementProxyImpl() {
        super("dcp");
    }

    private String encodeURLParameter(String parameter) {
        log.info("Attempting to encode parameter " + parameter);
        String encodedParameter = null;
        try {
            encodedParameter = URLEncoder.encode(parameter, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            log.error("Unexpected error encoding URL parameter " + parameter, e);
            encodedParameter = "ALL";
        }
        log.info("Encoded parameter " + parameter + " as " + encodedParameter);
        return encodedParameter;
    }

    @Override
    public DataBlockEntitlementContainer getEntitlement(String customerSpace, String domainName, String recordType) {
        String url = constructUrl("/customerspaces/{customerSpace}/entitlement/{domainName}/{recordType}", //
                shortenCustomerSpace(customerSpace), encodeURLParameter(domainName), encodeURLParameter(recordType));
        return get("get entitlement", url, DataBlockEntitlementContainer.class);
    }

}
