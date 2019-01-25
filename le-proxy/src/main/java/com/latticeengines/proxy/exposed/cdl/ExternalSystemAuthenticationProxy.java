package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("externalSystemAuthenticationProxy")
public class ExternalSystemAuthenticationProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(ExternalSystemAuthenticationProxy.class);

    private static final String URL_PARAM_TEMPLATE = "&%s={%s}";
    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/external-system-authentication";

    protected ExternalSystemAuthenticationProxy() {
        super("cdl");
    }

    public ExternalSystemAuthentication createAuthentication(String customerSpace,
            ExternalSystemAuthentication externalSystemAuthentication) {
        String url = constructUrl(URL_PREFIX + "/", shortenCustomerSpace(customerSpace));
        return post("createExternalSystemAuthentication", url, externalSystemAuthentication,
                ExternalSystemAuthentication.class);
    }

    public ExternalSystemAuthentication updateAuthentication(String customerSpace, String authId,
            ExternalSystemAuthentication externalSystemAuthentication) {
        String url = constructUrl(URL_PREFIX + "/{authId}", shortenCustomerSpace(customerSpace), authId);
        return put("updateExternalSystemAuthentication", url, externalSystemAuthentication,
                ExternalSystemAuthentication.class);
    }

    public ExternalSystemAuthentication findAuthenticationByAuthId(String customerSpace, String authId) {
        String url = constructUrl(URL_PREFIX + "/{authId}", shortenCustomerSpace(customerSpace), authId);
        return get("findExternalSystemAuthenticationBuAuthId", url, ExternalSystemAuthentication.class);
    }

    public List<ExternalSystemAuthentication> findAuthentications(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/", shortenCustomerSpace(customerSpace));
        return getList("findAllExternalSystemAuthentication", url, ExternalSystemAuthentication.class);
    }

    public List<ExternalSystemAuthentication> findAuthenticationsByLookupMapIds(String customerSpace,
            List<String> lookupIdMappings) {
        if (lookupIdMappings == null || lookupIdMappings.isEmpty()) {
            throw new IllegalArgumentException("LookupIdMappings cannot be empty");
        }

        StringBuilder sb = new StringBuilder(URL_PREFIX + "/lookupid-mappings?");
        List<Object> paramObjects = new ArrayList<>();
        paramObjects.addAll(Arrays.asList(shortenCustomerSpace(customerSpace)));

        StringBuilder paramBuilder = new StringBuilder();
        addParameter("mapping_ids", lookupIdMappings, paramBuilder, paramObjects);

        String url = constructUrl(sb.append(paramBuilder.substring(1)).toString(),
                paramObjects.toArray(new Object[paramObjects.size()]));
        log.info("Find Auths by LookupIDMappings: {}", url);
        return getList("findAllExternalSystemAuthenticationByLookupMappings", url, ExternalSystemAuthentication.class);
    }

    protected void addParameter(String paramName, Object value, StringBuilder sb, List<Object> paramObjects) {
        if (value == null) {
            return;
        }
        if (value instanceof String && StringUtils.isNotBlank((String) value)) {
            sb.append(String.format(URL_PARAM_TEMPLATE, paramName, paramName));
            paramObjects.add(((String) value).trim());
        } else if (value instanceof Boolean) {
            sb.append(String.format(URL_PARAM_TEMPLATE, paramName, paramName));
            paramObjects.add(value);
        } else if (value instanceof List && CollectionUtils.isNotEmpty((List<?>) value)) {
            ((List<?>) value).stream().forEach(v -> {
                sb.append(String.format(URL_PARAM_TEMPLATE, paramName, paramName));
                if (v instanceof String) {
                    paramObjects.add(((String) v).trim());
                } else {
                    paramObjects.add(v);
                }
            });
        }
        
    }
}
