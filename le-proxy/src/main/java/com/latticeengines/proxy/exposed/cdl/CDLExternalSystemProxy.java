package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("cdlExternalSystemProxy")
public class CDLExternalSystemProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/cdlexternalsystem";

    protected CDLExternalSystemProxy() {
        super("cdl");
    }

    public CDLExternalSystem getCDLExternalSystem(String customerSpace, String entity) {
        String url = constructUrl(URL_PREFIX + "?entity={entity}", shortenCustomerSpace(customerSpace), entity);
        return get("get CDL external system", url, CDLExternalSystem.class);
    }

    public void createOrUpdateCDLExternalSystem(String customerSpace, CDLExternalSystem cdlExternalSystem) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        post("create or update a CDL external system", url, cdlExternalSystem, Void.class);
    }

    public List<CDLExternalSystemMapping> getExternalSystemByType(String customerSpace, CDLExternalSystemType type) {
        String url = constructUrl(URL_PREFIX + "/type/{type}", shortenCustomerSpace(customerSpace), type.name());
        List<?> res = get("Get external system by type", url, List.class);
        return JsonUtils.convertList(res, CDLExternalSystemMapping.class);
    }

    public Map<String, List<CDLExternalSystemMapping>> getExternalSystemMap(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/map", shortenCustomerSpace(customerSpace));
        Map<?, ?> res = get("Get external system mapping map", url, Map.class);
        Map<String, List> raw = JsonUtils.convertMap(res, String.class, List.class);
        Map<String, List<CDLExternalSystemMapping>> mappings = new HashMap<>();
        for (String key : raw.keySet()) {
            mappings.put(key, JsonUtils.convertList(raw.get(key), CDLExternalSystemMapping.class));
        }
        return mappings;
    }

}
