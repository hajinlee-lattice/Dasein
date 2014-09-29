package com.latticeengines.perf.rest.client;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.perf.job.runnable.impl.LoadData;
import com.latticeengines.perf.yarn.configuration.dao.Property;
import com.latticeengines.perf.yarn.configuration.dao.YarnConfigurationSettings;

public class LedpRestClient {

	private RestTemplate rt = new RestTemplate();
	private RetryTemplate rtt = new RetryTemplate();
	private static final Log log = LogFactory.getLog(LedpRestClient.class);
	private String restEndpointHost;

	public LedpRestClient() {
		rt.setErrorHandler(new ThrowExceptionResponseErrorHandler());
	}

	public LedpRestClient(String restEndpointHost) {
		this.restEndpointHost = restEndpointHost;
		rt.setErrorHandler(new ThrowExceptionResponseErrorHandler());
	}

	public AppSubmission retryRequest(final String url, final Object config)
			throws Exception {
		AppSubmission submission = rtt
				.execute(new RetryCallback<AppSubmission, Exception>() {
					public AppSubmission doWithRetry(RetryContext context) {
						return rt.postForObject(url, config,
								AppSubmission.class, new Object[] {});
					}
				});
		return submission;
	}

	public List<String> loadData(final LoadConfiguration config)
			throws Exception {
		AppSubmission submission = retryRequest("http://" + restEndpointHost
				+ "/rest/load", config);
		List<String> applicationIds = submission.getApplicationIds();
		log.info(applicationIds);
		return applicationIds;
	}

	public List<String> createSamples(SamplingConfiguration config)
			throws Exception {
		AppSubmission submission = rt.postForObject("http://"
				+ restEndpointHost + "/rest/createSamples", config,
				AppSubmission.class, new Object[] {});
		List<String> applicationIds = submission.getApplicationIds();
		log.info(applicationIds);
		return applicationIds;
	}

	public List<String> profile(DataProfileConfiguration config)
			throws Exception {
		AppSubmission submission = rt.postForObject("http://"
				+ restEndpointHost + "/rest/profile", config,
				AppSubmission.class, new Object[] {});
		List<String> applicationIds = submission.getApplicationIds();
		log.info(applicationIds);
		return applicationIds;
	}

	public List<String> submitModel(Model model) throws Exception {
		AppSubmission submission = rt.postForObject("http://"
				+ restEndpointHost + "/rest/submit", model,
				AppSubmission.class, new Object[] {});
		List<String> applicationIds = submission.getApplicationIds();
		log.info(applicationIds);
		return applicationIds;
	}

	public List<String> getFeatures(Model model) throws Exception {
		StringList features = rt.postForObject("http://" + restEndpointHost
				+ "/rest/features", model, StringList.class, new Object[] {});
		return features.getElements();
	}

	public JobStatus getJobStatus(final String appId, String hdfsPath)
			throws Exception {
		JobStatus js = rt.postForObject("http://" + restEndpointHost
				+ "/rest/getJobStatus/" + appId, hdfsPath, JobStatus.class,
				new HashMap<String, Object>());
		return js;
    }

	public JobStatus getJobStatus(final String appId) throws Exception {
		JobStatus js = rt.getForObject("http://" + restEndpointHost
				+ "/rest/getJobStatus/" + appId, JobStatus.class,
				new HashMap<String, Object>());
		return js;
	}

	public List<Property> getYarnConfiguration(String RMHostAddress)
			throws JAXBException {
		String s = rt.getForObject("http://" + RMHostAddress + "/conf",
				String.class);
		YarnConfigurationSettings ycs = (YarnConfigurationSettings) JAXBContext
				.newInstance(YarnConfigurationSettings.class)
				.createUnmarshaller().unmarshal(new StringReader(s));
		return ycs.getProperties();
	}

	class ThrowExceptionResponseErrorHandler implements ResponseErrorHandler {

		@Override
		public boolean hasError(ClientHttpResponse response) throws IOException {
			if (response.getStatusCode() == HttpStatus.OK) {
				return false;
			}
			return true;
		}

		@Override
		public void handleError(ClientHttpResponse response) throws IOException {

			String responseBody = IOUtils.toString(response.getBody());

			log.info("Error response from rest call: "
					+ response.getStatusCode() + " " + response.getStatusText()
					+ " " + responseBody);
			throw new RuntimeException(responseBody);
		}
	}

	public static void main(String[] args) {
		LedpRestClient lrc = new LedpRestClient("localhost:8080");
		// System.out.println(lrc.getYarnConfiguration("localhost:8088").get(1).getName());
		try {
			// List<String> appIds = lrc.loadData(new LoadConfiguration());
			// System.out.println(appIds.size());
			LoadData ld = new LoadData();
			ld.setConfiguration("localhost:8080", new LoadConfiguration());
			List<String> appIds = ld.executeJob();
		} catch (Exception e) {
			// TODO Auto-generated catch block

		}
		// System.out.println(le.getCode());
	}
}
