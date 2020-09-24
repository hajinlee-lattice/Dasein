package com.latticeengines.proxy.exposed.cdl;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("cdlExternalSystemNameProxy")
public class CDLExternalSystemNameProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected CDLExternalSystemNameProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public Map<BusinessEntity, List<CDLExternalSystemName>> getExternalSystemNames() {
        String url = constructUrl("/cdlexternalsystemname");
        Map<?, List<?>> rawSystemNames = get("getExternalSystemNames", url, Map.class);
        return JsonUtils.convertMapWithListValue(rawSystemNames, BusinessEntity.class, CDLExternalSystemName.class);
    }

    public List<CDLExternalSystemName> getExternalSystemNamesForLiveRamp() {
        String url = constructUrl("/cdlexternalsystemname/liveramp");
        List<?> rawSystemNames = get("getExternalSystemNamesForLiveRamp", url, List.class);
        return JsonUtils.convertList(rawSystemNames, CDLExternalSystemName.class);
    }

}
