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
import org.apache.hadoop.mapreduce.v2.app.LedpMRAppMaster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.service.MapReduceCustomizationService;
import com.latticeengines.dataplatform.service.YarnClientCustomizationService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("jobService")
public class JobServiceImpl implements JobService, ApplicationContextAware {

    protected static final Log log = LogFactory.getLog(JobServiceImpl.class);
    protected static final int MAX_TRIES = 60;
    protected static final long APP_WAIT_TIME = 1000L;

    private ApplicationContext applicationContext;

    @Autowired
    protected YarnClient defaultYarnClient;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected Configuration hadoopConfiguration;

    @Autowired
    private MapReduceCustomizationService mapReduceCustomizationService;

    @Autowired
    private YarnClientCustomizationService yarnClientCustomizationService;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Override
    public List<ApplicationReport> getJobReportsAll() {
        return defaultYarnClient.listApplications();
    }

    @Override
    public ApplicationReport getJobReportById(ApplicationId appId) {
        return defaultYarnClient.getApplicationReport(appId);
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
    public ApplicationId submitYarnJob(String yarnClientName, Properties appMasterProperties, Properties containerProperties) {
        CommandYarnClient client = (CommandYarnClient) getYarnClient(yarnClientName);
        yarnClientCustomizationService.validate(client, yarnClientName, appMasterProperties, containerProperties);
        yarnClientCustomizationService.addCustomizations(client, yarnClientName, appMasterProperties, containerProperties);

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
            config.set("yarn.mr.am.class.name", LedpMRAppMaster.class.getName());
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
            throw new LedpException(LedpCode.LEDP_12009, e, new String[] { mrJobName });
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
    public ApplicationId submitJob(com.latticeengines.domain.exposed.dataplatform.Job job) {
        return submitYarnJob(job.getClient(), job.getAppMasterPropertiesObject(), job.getContainerPropertiesObject());
    }

    @Override
    public void populateJobStatusFromYarnAppReport(JobStatus jobStatus, String applicationId) {
        ApplicationReport appReport = getJobReportById(ConverterUtils.toApplicationId(applicationId));
        jobStatus.setId(applicationId);
        if (appReport != null) {
            jobStatus.setStatus(appReport.getFinalApplicationStatus());
            jobStatus.setState(appReport.getYarnApplicationState());
            jobStatus.setDiagnostics(appReport.getDiagnostics());
            jobStatus.setFinishTime(appReport.getFinishTime());
            jobStatus.setProgress(appReport.getProgress());
            jobStatus.setStartTime(appReport.getStartTime());
            jobStatus.setTrackingUrl(appReport.getTrackingUrl());
            jobStatus.setAppResUsageReport(appReport.getApplicationResourceUsageReport());
        }
    }

    @Override
    public void createHdfsDirectory(String directory, boolean errorIfExists) {
        try {
            HdfsUtils.mkdir(yarnConfiguration, directory);
        } catch (Exception e) {
            if (errorIfExists) {
                throw new LedpException(LedpCode.LEDP_00000, e, new String[] { directory });
            } else {
                log.warn(LedpException.buildMessage(LedpCode.LEDP_00000, new String[] { directory }));
            }
        }
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        JobStatus jobStatus = new JobStatus();
        populateJobStatusFromYarnAppReport(jobStatus, applicationId);
        return jobStatus;
    }

}
