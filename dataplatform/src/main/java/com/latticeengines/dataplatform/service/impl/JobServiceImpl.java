package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.runtime.execution.python.PythonContainerProperty;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.MapReduceCustomizationService;
import com.latticeengines.dataplatform.service.YarnClientCustomizationService;
import com.latticeengines.dataplatform.util.HdfsHelper;

@Component("jobService")
public class JobServiceImpl implements JobService, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(JobServiceImpl.class);

    private ApplicationContext applicationContext;

    @Autowired
    private YarnClient defaultYarnClient;
    
    @Autowired
    private MapReduceCustomizationService mapReduceCustomizationService;

    @Autowired
    private YarnClientCustomizationService yarnClientCustomizationService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Override
    public List<ApplicationReport> getJobReportsAll() {
        return defaultYarnClient.listApplications();
    }

    @Override
    public ApplicationReport getJobReportById(ApplicationId appId) {
        List<ApplicationReport> reports = getJobReportsAll();
        for (ApplicationReport report : reports) {
            if (report != null && report.getApplicationId() != null && report.getApplicationId().equals(appId)) {
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
            if (report != null && report.getUser() != null && report.getUser().equalsIgnoreCase(user)) {
                userReports.add(report);
            }
        }
        return userReports;
    }

    @Override
    public ApplicationId submitYarnJob(String yarnClientName, Properties appMasterProperties,
            Properties containerProperties) {
        CommandYarnClient client = (CommandYarnClient) getYarnClient(yarnClientName);
        yarnClientCustomizationService.validate(client, yarnClientName, appMasterProperties, containerProperties);
        yarnClientCustomizationService.addCustomizations(client, yarnClientName, appMasterProperties,
                containerProperties);

        try {
            ApplicationId applicationId = client.submitApplication();
            return applicationId;
        } finally {
            yarnClientCustomizationService.finalize(yarnClientName, appMasterProperties, containerProperties);
        }
    }

    @Override
    public void killJob(ApplicationId appId) {
        defaultYarnClient.killApplication(appId);
    }

    private YarnClient getYarnClient(String yarnClientName) {
        ConfigurableApplicationContext context = null;
        try {
            if (StringUtils.isEmpty(yarnClientName)) {
                throw new IllegalStateException("Yarn client name cannot be empty.");
            }
            YarnClient client = (YarnClient) applicationContext.getBean(yarnClientName);
            return client;
        } catch (Throwable e) {
            log.error("Error while getting yarnClient for application " + yarnClientName, e);
        } finally {
            if (context != null) {
                context.close();
            }
        }
        return null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public ApplicationId submitMRJob(String mrJobName, Properties properties) {
        Job job = getJob(mrJobName);
        mapReduceCustomizationService.addCustomizations(job, mrJobName, properties);
        if (properties != null) {
            Configuration config = job.getConfiguration();
            for (Object key : properties.keySet()) {
                config.set(key.toString(), properties.getProperty((String) key));
            }
        }

        JobRunner runner = new JobRunner();
        runner.setJob(job);
        try {
            runner.setWaitForCompletion(false);
            runner.call();
        } catch (Exception e) {
            log.error("Failed to submit MapReduce job " + mrJobName, e);
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
            log.error("Error while getting hdp:job " + mrJobName, e);
        } finally {
            if (context != null) {
                context.close();
            }
        }
        return null;
    }

    @Override
    public ApplicationId submitJob(com.latticeengines.dataplatform.exposed.domain.Job job) {
        ApplicationId appId = submitYarnJob(job.getClient(), job.getAppMasterProperties(), job.getContainerProperties());
        job.setId(appId.toString());
        jobEntityMgr.post(job);
        jobEntityMgr.save();
        return appId;
    }
    
    @Override
    public ApplicationId resubmitPreemptedJob(com.latticeengines.dataplatform.exposed.domain.Job job) {
        if (job.getChildJobIds().size() > 0) {
            if (log.isDebugEnabled())  {
                log.debug("Did not resubmit preempted job " + job.getId() + ". Already resubmitted.");
            }
            
            return null;
        }
        String metadata = job.getContainerProperties()
                .getProperty(PythonContainerProperty.METADATA_CONTENTS.name());
        job.getContainerProperties().setProperty(PythonContainerProperty.METADATA.name(), metadata);
        String parentId = job.getId();
        job.setId(null);
        job.setParentJobId(parentId);
        ApplicationId appId = submitJob(job);
        log.info("Resubmitted " + parentId + " with " + job.getId() + ".");
        com.latticeengines.dataplatform.exposed.domain.Job parentJob = jobEntityMgr.getById(parentId);
        parentJob.addChildJobId(job.getId());
        jobEntityMgr.post(parentJob);
        return appId;
    }

	@Override
	public void createHdfsDirectory(String directory, boolean errorIfExists) {
		try {
			HdfsHelper.mkdir(yarnConfiguration, directory);
		} catch (Exception e) {
			if (errorIfExists) {
				throw new LedpException(LedpCode.LEDP_00000, e, new String[] { directory });	
			} else {
				log.warn(LedpException.buildMessage(LedpCode.LEDP_00000, new String[] { directory }));
			}
		}
	}

}
