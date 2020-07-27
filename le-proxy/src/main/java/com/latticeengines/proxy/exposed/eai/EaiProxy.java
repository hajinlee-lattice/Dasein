package com.latticeengines.proxy.exposed.eai;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("eaiProxy")
public class EaiProxy extends MicroserviceRestApiProxy {

    public EaiProxy() {
        super("eai");
    }

    public AppSubmission submitEaiJob(EaiJobConfiguration eaiJobConfig) {
        String url = constructUrl("/jobs");
        return post("createEaiJob", url, eaiJobConfig, AppSubmission.class);
    }
}
