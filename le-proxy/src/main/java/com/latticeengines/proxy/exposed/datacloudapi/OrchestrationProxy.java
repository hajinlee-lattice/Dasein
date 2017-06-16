package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.network.exposed.propdata.OrchestrationInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("orchestrationProxy")
public class OrchestrationProxy extends MicroserviceRestApiProxy implements OrchestrationInterface {

    public OrchestrationProxy() {
        super("datacloudapi/orchestrations");
    }

    @Override
    public List<OrchestrationProgress> scan(String hdfsPod) {
        String url = constructUrl("/?HdfsPod={hdfsPod}", hdfsPod);
        List<?> list = post("scan_orchestration", url, "", List.class);
        return JsonUtils.convertList(list, OrchestrationProgress.class);
    }

}
