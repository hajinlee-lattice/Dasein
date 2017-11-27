package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("cdlExternalSystemProxy")
public class CDLExternalSystemProxy extends MicroserviceRestApiProxy {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/cdlexternalsystem";

    protected CDLExternalSystemProxy() {
        super("cdl");
    }

    public List<CDLExternalSystem> getCDLExternalSystems(String customerSpace) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        List list = get("get cdl external systems.", url, List.class);
        return JsonUtils.convertList(list, CDLExternalSystem.class);
    }

    public void createCDLExternalSystem(String customerSpace, String systemType, InterfaceName accountInterface) {
        String url = constructUrl(URL_PREFIX + "/{systemType}/accountinterface/{accountInterface}",
                shortenCustomerSpace(customerSpace), systemType, accountInterface);
        post("create cdl external system", url, null, Void.class);
    }

    public void createCDLExternalSystem(String customerSpace, InterfaceName crmAccountInterface,
                                        InterfaceName mapAccountInterface, InterfaceName erpAccountInterface) {
        String url = constructUrl(URL_PREFIX, shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (crmAccountInterface != null) {
            params.add("crmAccount=" + crmAccountInterface.name());
        }
        if (mapAccountInterface != null) {
            params.add("mapAccount=" + mapAccountInterface.name());
        }
        if (erpAccountInterface != null) {
            params.add("erpAccount=" + erpAccountInterface.name());
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        post("create cdl external system", url, null, Void.class);
    }

}
