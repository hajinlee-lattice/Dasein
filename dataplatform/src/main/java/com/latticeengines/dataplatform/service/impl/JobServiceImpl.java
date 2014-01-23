package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;
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
	public ApplicationId submitYarnJob(String yarnClientName) {
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

	@Override
	public ApplicationId submitMRJob(String mrJobName) {
		Job job = getJob(mrJobName);
		JobRunner runner = new JobRunner();
		runner.setJob(job);
		try {
			runner.setWaitForCompletion(false);
			runner.call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("failed to submit MapReduce job " + mrJobName);
			e.printStackTrace();
		}
		return TypeConverter.toYarn(job.getJobID()).getAppId();
	}

	private Job getJob(String mrJobName) {
		ConfigurableApplicationContext context = null;
		try {
			if (StringUtils.isEmpty(mrJobName)) {
				throw new IllegalStateException("MapReduce job name cannot be empty");
			}
			Job job = (Job) applicationContext.getBean(mrJobName);
			// clone the job
			job = Job.getInstance(job.getConfiguration());
			return job;
		} catch (Throwable e) {
			log.error("Error happend while getting hdp:job " + mrJobName, e);
		} finally {
			if (context != null) {
				context.close();
			}
		}
		return null;
	}

}
