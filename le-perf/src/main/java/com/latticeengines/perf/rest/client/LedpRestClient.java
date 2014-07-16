package com.latticeengines.perf.rest.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.perf.yarn.configuration.dao.Property;
import com.latticeengines.perf.yarn.configuration.dao.YarnConfigurationSettings;

public class LedpRestClient {

    private RestTemplate rt = new RestTemplate();
    private static final Log log = LogFactory.getLog(LedpRestClient.class);
    private String restEndpointHost;

    public LedpRestClient() {
    }

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

    public List<Property> getYarnConfiguration(String RMHostAddress) throws JAXBException {
        String s = rt.getForObject("http://" + RMHostAddress + "/conf", String.class);
        YarnConfigurationSettings ycs = (YarnConfigurationSettings) JAXBContext
                .newInstance(YarnConfigurationSettings.class).createUnmarshaller().unmarshal(new StringReader(s));
        return ycs.getProperties();
    }

    public static void main(String[] args) throws JAXBException, IOException {
        LedpRestClient lrc = new LedpRestClient();
        // System.out.println(lrc.getYarnConfiguration("localhost:8088").get(1).getName());

        String url = "jdbc:sqlserver://10.41.1.250:1433;databaseName=ledp_dev";

        String str = url.substring(url.indexOf("//") + 2);
        System.out.println(str.split(":")[0]);
        System.out.println(str.split(":")[1].split(";")[0]);
        System.out.println(str.split(":")[1].split(";")[1].split("=")[1]);
    }
}
