package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.dataplatform.service.JobService;

@Component("jobService")
public class JobServiceImpl implements JobService, ApplicationContextAware {
	
	private static final Log log = LogFactory.getLog(JobServiceImpl.class);
	
	private ApplicationContext applicationContext;
	
	@Autowired
	private YarnClient yarnClient;

	@Override
	public List<ApplicationReport> getJobReportsAll() {
		return yarnClient.listApplications();
	}

	@Override
	public ApplicationReport getJobReportById(ApplicationId appId) {
		List<ApplicationReport> reports = getJobReportsAll();
		for (ApplicationReport report : reports) {
			if (report != null && report.getApplicationId() != null 
				&& report.getApplicationId().equals(appId)) {
				return report;
			}
		}
		return null;
	}

	@Override
	public List<ApplicationReport> getJobReportByUser(String user) {
		List<ApplicationReport> reports = getJobReportsAll();
		List<ApplicationReport> userReports = new ArrayList<ApplicationReport>();
		for (ApplicationReport report : reports) {
			if (report != null && report.getUser() != null 
				&& report.getUser().equalsIgnoreCase(user)) {
				userReports.add(report);
			}
		}
		return userReports;
	}

	@Override
	public ApplicationId submitJob(String yarnClientName) {
		YarnClient client = getYarnClient(yarnClientName);
		ApplicationId applicationId = client.submitApplication();
		return applicationId;
	}

	@Override
	public void killJob(ApplicationId appId) {
		yarnClient.killApplication(appId);
	}

	private YarnClient getYarnClient(String yarnClientName) {
		ConfigurableApplicationContext context = null;
		try {
			if (StringUtils.isEmpty(yarnClientName)) {
				throw new IllegalStateException("yarn client name cannot be empty");
			}
			YarnClient client = (YarnClient) applicationContext.getBean(yarnClientName);
			return client;
		} catch (Throwable e) {
			log.error("Error happend while getting yarnClient for application " + yarnClientName, e);
		} finally {
			if (context != null) {
				context.close();
			}
		}
		return null;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = applicationContext;
	}
}
