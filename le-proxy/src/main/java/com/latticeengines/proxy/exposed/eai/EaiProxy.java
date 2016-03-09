package com.latticeengines.proxy.exposed.eai;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.network.exposed.eai.EaiInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("eaiProxy")
public class EaiProxy extends BaseRestApiProxy implements EaiInterface {

    public EaiProxy() {
        super("eai/importjobs");
    }

    @Override
    public AppSubmission createImportDataJob(ImportConfiguration importConfig) {
        String url = constructUrl("/");
        return post("createImportDataJob", url, importConfig, AppSubmission.class);
    }

    @Override
    public JobStatus getImportDataJobStatus(String applicationId) {
        String url = constructUrl("{applicationId}", applicationId);
        return get("getImportDataJobStatus", url, JobStatus.class);
    }

}
