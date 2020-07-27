package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("orchestrationProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class OrchestrationProxy extends MicroserviceRestApiProxy {

    public OrchestrationProxy() {
        super("datacloudapi/orchestrations");
    }

    public List<OrchestrationProgress> scan(String hdfsPod) {
        String url = constructUrl("/?HdfsPod={hdfsPod}", hdfsPod);
        List<?> list = post("scan_orchestration", url, "", List.class);
        return JsonUtils.convertList(list, OrchestrationProgress.class);
    }

}
