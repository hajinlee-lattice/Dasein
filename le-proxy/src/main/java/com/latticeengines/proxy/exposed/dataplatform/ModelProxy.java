package com.latticeengines.proxy.exposed.dataplatform;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
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

    @Override
    public AppSubmission loadData(LoadConfiguration config) {
        String url = constructUrl("/dataloads");
        return post("loadData", url, config, AppSubmission.class);
    }

    @Override
    public AppSubmission exportData(ExportConfiguration config) {
        String url = constructUrl("/dataexports");
        return post("exportData", url, config, AppSubmission.class);
    }

    @Override
    public StringList getFeatures(Model model) {
        String url = constructUrl("/features");
        return post("getFeatures", url, model, StringList.class);
    }

    @Override
    public AppSubmission submit(Model model) {
        String url = constructUrl("/models");
        return post("submit", url, model, AppSubmission.class);
    }

    @Override
    public AppSubmission profile(DataProfileConfiguration config) {
        String url = constructUrl("/profiles");
        return post("profile", url, config, AppSubmission.class);
    }

    @Override
    public AppSubmission createSamples(SamplingConfiguration config) {
        String url = constructUrl("/samples");
        return post("createSamples", url, config, AppSubmission.class);
    }
}
