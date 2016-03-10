package com.latticeengines.proxy.exposed.dataplatform;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.network.exposed.dataplatform.ModelInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("modelProxy")
public class ModelProxy extends BaseRestApiProxy implements ModelInterface {

    public ModelProxy() {
        super("modeling");
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        String url = constructUrl("/modelingjobs/{applicationId}", applicationId);
        return get("getJobStatus", url, JobStatus.class);
    }

}
