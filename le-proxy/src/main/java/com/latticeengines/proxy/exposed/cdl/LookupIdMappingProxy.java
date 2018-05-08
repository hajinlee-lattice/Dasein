package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("lookupIdMappingProxy")
public class LookupIdMappingProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(LookupIdMappingProxy.class);

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/lookup-id-mapping";

    protected LookupIdMappingProxy() {
        super("cdl");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<String, List<LookupIdMap>> getLookupIdsMapping(String customerSpace,
            CDLExternalSystemType externalSystemType, String sortby, boolean descending) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (externalSystemType != null) {
            params.add("externalSystemType=" + externalSystemType);
        }
        if (StringUtils.isNotEmpty(sortby)) {
            params.add("sortby=" + sortby.trim());
        }
        params.add("descending=" + descending);

        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("url is " + url);
        Map lookupIdConfigsRaw = (Map) get("getLookupIdsMapping", url, Map.class);
        if (lookupIdConfigsRaw == null) {
            return new HashMap<>();
        }
        return JsonUtils.convertMapWithListValue(lookupIdConfigsRaw, String.class, LookupIdMap.class);
    }

    public LookupIdMap registerExternalSystem(String customerSpace, LookupIdMap lookupIdMap) {
        String url = constructUrl(URL_PREFIX + "/register", shortenCustomerSpace(customerSpace));
        return post("registerExternalSystem", url, lookupIdMap, LookupIdMap.class);
    }

    public void deregisterExternalSystem(String customerSpace, LookupIdMap lookupIdMap) {
        String url = constructUrl(URL_PREFIX + "/deregister", shortenCustomerSpace(customerSpace));
        put("deregisterExternalSystem", url, lookupIdMap);
    }

    public LookupIdMap getLookupIdMap(String customerSpace, String id) {
        String url = constructUrl(URL_PREFIX + "/config/{id}", shortenCustomerSpace(customerSpace), id);
        return get("getLookupIdMap", url, LookupIdMap.class);
    }

    public LookupIdMap updateLookupIdMap(String customerSpace, String id, LookupIdMap lookupIdMap) {
        String url = constructUrl(URL_PREFIX + "/config/{id}", shortenCustomerSpace(customerSpace), id);
        return put("updateLookupIdMap", url, lookupIdMap, LookupIdMap.class);
    }

    public void deleteLookupIdMap(String customerSpace, String id) {
        String url = constructUrl(URL_PREFIX + "/config/{id}", shortenCustomerSpace(customerSpace), id);
        delete("getLookupIdMap", url);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(String customerSpace,
            CDLExternalSystemType externalSystemType) {
        String url = constructUrl(URL_PREFIX + "/available-lookup-ids", shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (externalSystemType != null) {
            params.add("externalSystemType=" + externalSystemType);
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        log.info("url is " + url);
        Map allLookupIdsRaw = (Map) get("getAllLookupIds", url, Map.class);
        if (allLookupIdsRaw == null) {
            return new HashMap<>();
        }
        return JsonUtils.convertMapWithListValue(allLookupIdsRaw, String.class, CDLExternalSystemMapping.class);
    }

    public List<CDLExternalSystemType> getAllCDLExternalSystemType(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/all-external-system-types", shortenCustomerSpace(customerSpace));
        List<?> allCDLExternalSystemTypeRaw = get("getAllLookupIds", url, List.class);
        if (allCDLExternalSystemTypeRaw == null) {
            return new ArrayList<>();
        }

        return JsonUtils.convertList(allCDLExternalSystemTypeRaw, CDLExternalSystemType.class);
    }
}
