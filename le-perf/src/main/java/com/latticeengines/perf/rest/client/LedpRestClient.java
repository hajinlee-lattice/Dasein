package com.latticeengines.perf.rest.client;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;

public class LedpRestClient {

    private RestTemplate rt = new RestTemplate();
    private static final Log log = LogFactory.getLog(LedpRestClient.class);
    private String restEndpointHost;

    public LedpRestClient(String restEndpointHost) {
        this.restEndpointHost = restEndpointHost;
    }

    public List<String> createSamples(SamplingConfiguration samplingConfig) throws Exception {
        AppSubmission submission = rt.postForObject("http://" + restEndpointHost + "/rest/createSamples",
                samplingConfig, AppSubmission.class, new Object[] {});
        List<String> applicationIds = submission.getApplicationIds();
        log.info(applicationIds);
        return applicationIds;
    }

    public List<String> loadData(LoadConfiguration config) throws Exception {
        AppSubmission submission = rt.postForObject("http://" + restEndpointHost + "/rest/load", config,
                AppSubmission.class, new Object[] {});
        List<String> applicationIds = submission.getApplicationIds();
        log.info(applicationIds);
        return applicationIds;
    }

    public List<String> profile(DataProfileConfiguration config) throws Exception {
        AppSubmission submission = rt.postForObject("http://" + restEndpointHost + "/rest/profile", config,
                AppSubmission.class, new Object[] {});
        List<String> applicationIds = submission.getApplicationIds();
        log.info(applicationIds);
        return applicationIds;
    }

    public List<String> submitModel(Model model) throws Exception {
        AppSubmission submission = rt.postForObject("http://" + restEndpointHost + "/rest/submit", model,
                AppSubmission.class, new Object[] {});
        List<String> applicationIds = submission.getApplicationIds();
        log.info(applicationIds);
        return applicationIds;
    }

    public List<String> getFeatures(Model model) throws Exception {
        StringList features = rt.postForObject("http://" + restEndpointHost + "/rest/features", model,
                StringList.class, new Object[] {});
        return features.getElements();
    }

    public JobStatus getJobStatus(String appId) throws Exception {
        JobStatus js = rt.getForObject("http://" + restEndpointHost + "/rest/getjobstatus/" + appId, JobStatus.class,
                new HashMap<String, Object>());
        return js;
    }

}
