package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;

@Component("plsCDLS3ImportProxy")
public class PlsCDLS3ImportProxy extends PlsRestApiProxyBase {

    public PlsCDLS3ImportProxy() {
        super("pls/cdl/s3import");
    }

    public List<S3ImportTemplateDisplay> getS3ImportTemplateEntries() {
        String url = constructUrl("/template");
        List<?> rawList = get("get all templates", url, List.class);
        return JsonUtils.convertList(rawList, S3ImportTemplateDisplay.class);
    }

    public Map<String, UIAction> createS3ImportSystem(String systemDisplayName, S3ImportSystem.SystemType systemType,
                                                      Boolean primary) {
        String url = constructUrl("/system?systemDisplayName={systemDisplayName}&systemType={systemType}&primary" +
                "={primary}", systemDisplayName, systemType, primary);
        Map map = post("create s3 import system", url, null, Map.class);
        return JsonUtils.convertMap(map, String.class, UIAction.class);
    }
}
