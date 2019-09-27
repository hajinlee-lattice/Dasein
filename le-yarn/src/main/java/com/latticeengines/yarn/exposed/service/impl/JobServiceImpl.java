package com.latticeengines.yarn.exposed.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.app.LedpMRAppMaster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.domain.exposed.mapreduce.counters.JobCounters;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonMRProperty;
import com.latticeengines.yarn.exposed.service.AwsBatchJobService;
import com.latticeengines.yarn.exposed.service.EMREnvService;
import com.latticeengines.yarn.exposed.service.JobService;
import com.latticeengines.yarn.exposed.service.MapReduceCustomizationService;
import com.latticeengines.yarn.exposed.service.YarnClientCustomizationService;

@Component("jobService")
public class JobServiceImpl implements JobService, ApplicationContextAware {

    // 60 * (5s delay + 10s connection timeout) = 15 minute
    private static final int MAX_CONTINUOUS_EXCEPTIONS = 60;

    protected static final Logger log = LoggerFactory.getLogger(JobServiceImpl.class);

    private static final EnumSet<FinalApplicationStatus> TERMINAL_STATUS = EnumSet.of(FinalApplicationStatus.FAILED,
            FinalApplicationStatus.KILLED, FinalApplicationStatus.SUCCEEDED);

    private ApplicationContext applicationContext;

    @Autowired
    protected YarnClient defaultYarnClient;

    @Autowired
    @Qualifier("yarnConfiguration")
    protected Configuration yarnConfiguration;

    @Autowired
    private MapReduceCustomizationService mapReduceCustomizationService;

    @Autowired
    private YarnClientCustomizationService yarnClientCustomizationService;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Inject
    private EMREnvService emrEnvService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private EMRCacheService emrCacheService;

    @Resource(name = "awsBatchjobService")
    private AwsBatchJobService awsBatchJobService;

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

    private String overwriteQueueInternal(String queue) {
        return LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
    }

    private void overwriteAMQueueAssignment(Properties appMasterProperties) {
        String queue = (String) appMasterProperties.get(AppMasterProperty.QUEUE.name());
        if (queue != null)
            appMasterProperties.put(AppMasterProperty.QUEUE.name(), overwriteQueueInternal(queue));
    }

    private void overwriteContainerQueueAssignment(Properties containerProperties) {
        String queue = (String) containerProperties.get("QUEUE");
        if (queue != null)
            containerProperties.put("QUEUE", overwriteQueueInternal(queue));
    }

    private void overwriteMRQueueAssignment(Properties mRProperties) {
        String queue = (String) mRProperties.get(MapReduceProperty.QUEUE.name());
        if (queue != null)
            mRProperties.put(MapReduceProperty.QUEUE.name(), overwriteQueueInternal(queue));
    }

    @Override
    public ApplicationId submitYarnJob(String yarnClientName, Properties appMasterProperties,
            Properties containerProperties) {

        overwriteAMQueueAssignment(appMasterProperties);
        overwriteContainerQueueAssignment(containerProperties);
        CommandYarnClient client = (CommandYarnClient) getYarnClient(yarnClientName);
        return submitYarnJob(client, yarnClientName, appMasterProperties, containerProperties);
    }

    @Override
    public ApplicationId submitYarnJob(CommandYarnClient client, String yarnClientName, Properties appMasterProperties,
            Properties containerProperties) {
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
    public ApplicationId submitAwsBatchJob(com.latticeengines.domain.exposed.dataplatform.Job job) {
        return awsBatchJobService.submitAwsBatchJob(job);
    }

    @Override
    public JobStatus getAwsBatchJobStatus(String jobId) {
        return awsBatchJobService.getAwsBatchJobStatus(jobId);
    }

    @Override
    public void killJob(ApplicationId appId) {
        defaultYarnClient.killApplication(appId);
    }

    @Override
    public YarnClient getYarnClient(String yarnClientName) {
        ConfigurableApplicationContext context = null;
        try {
            if (StringUtils.isEmpty(yarnClientName)) {
                throw new IllegalStateException("Yarn client name cannot be empty.");
            }
            CommandYarnClient client = (CommandYarnClient) applicationContext.getBean(yarnClientName);
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
        overwriteMRQueueAssignment(properties);
        Job job = getJob(mrJobName);
        mapReduceCustomizationService.addCustomizations(job, mrJobName, properties);
        if (job != null) {
            Configuration config = job.getConfiguration();
            config.set("yarn.mr.am.class.name", LedpMRAppMaster.class.getName());
            for (Object key : properties.keySet()) {
                config.set(key.toString(), properties.getProperty((String) key));
            }
        }

        Configuration config = job.getConfiguration();
        log.info(String.format("Job %s Properties:", mrJobName));
        if (Boolean.TRUE.equals(useEmr)) { // useEmr might be null
            config.set(PythonMRProperty.SHDP_HD_FSWEB.name(), emrCacheService.getWebHdfsUrl());
        }
        config.forEach(entry -> log.debug(String.format("%s: %s", entry.getKey(), entry.getValue())));
        try {
            JobID jobId = JobService.runMRJob(job, mrJobName, false);
            return TypeConverter.toYarn(jobId).getAppId();
        } catch (Exception e) {
            log.error("Failed to submit MapReduce job " + mrJobName, e);
            throw new LedpException(LedpCode.LEDP_12009, e, new String[] { mrJobName });
        }
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
        ApplicationReport appReport = getJobReportById(ApplicationIdUtils.toApplicationIdObj(applicationId));
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

    @Override
    public JobStatus getJobStatusByCluster(String applicationId, String clusterId) {
        JobStatus jobStatus = new JobStatus();
        if (StringUtils.isNotEmpty(clusterId)) {
            try {
                try (org.apache.hadoop.yarn.client.api.YarnClient yarnClient = emrEnvService.getYarnClient(clusterId)) {
                    yarnClient.start();
                    ApplicationReport appReport =
                            yarnClient.getApplicationReport(ApplicationIdUtils.toApplicationIdObj(applicationId));
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
            } catch (IOException | YarnException e) {
                throw new RuntimeException(e);
            }
        } else {
            return getJobStatus(applicationId);
        }
        return jobStatus;
    }

    @Override
    public JobStatus waitFinalJobStatus(String applicationId, Integer timeoutInSec) {
        if (timeoutInSec == null) {
            timeoutInSec = 3600;
        }
        log.info(String.format("Wait %s for %d seconds", applicationId, timeoutInSec));
        JobStatus finalStatus = null;
        long startTime = System.currentTimeMillis();
        int nContExceptions = 0; // number of continuous exceptions
        do {
            try {
                finalStatus = getJobStatus(applicationId);
                String logMsg = String.format("Waiting for application [%s]: %s", applicationId,
                        finalStatus.getState());
                if (YarnApplicationState.RUNNING.equals(finalStatus.getState())) {
                    logMsg += String.format(" %f ", finalStatus.getProgress() * 100);
                }
                log.info(logMsg);
                // clear once we get status successfully
                nContExceptions = 0;
            } catch (Exception e) {
                nContExceptions++;
                log.warn(
                        "Failed to get application status of application id = {}," +
                        " # consecutive errors = {}, error = {}",
                        applicationId, nContExceptions, e.getMessage());
                if (nContExceptions >= MAX_CONTINUOUS_EXCEPTIONS) {
                    // failed for too many consecutive times
                    String msg = String.format(
                            "Failed to retrieve yarn job status for %d consecutive times, application ID = %s",
                            nContExceptions, applicationId);
                    log.error(msg, e);
                    throw new RuntimeException(msg);
                }
            }

            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }

            if ((System.currentTimeMillis() - startTime) >= timeoutInSec * 1000L) {
                break;
            }
        } while (finalStatus == null || !TERMINAL_STATUS.contains(finalStatus.getStatus()));

        log.info(
                String.format("The terminal status of application [%s] is %s", applicationId, finalStatus.getStatus()));

        return finalStatus;
    }

    @Override
    public Counters getMRJobCounters(String applicationId) {
        JobID jobId = TypeConverter.fromYarn(ConverterUtils.toApplicationId(applicationId));
        RestTemplate rt = new RestTemplate();
        String mrJobHistoryServerUrl = yarnConfiguration.get("mapreduce.jobhistory.webapp.address");
        mrJobHistoryServerUrl = String.format("http://%s/ws/v1/history/mapreduce/jobs", mrJobHistoryServerUrl);
        String jobUrl = mrJobHistoryServerUrl + "/" + jobId.toString() + "/counters";
        log.info("Requesting counters info from " + jobUrl);
        String response = rt.getForObject(jobUrl, String.class);
        JobCounters jobCounters = JsonUtils.deserialize(response, JobCounters.class);
        return jobCounters.getCounters();
    }


    @Override
    public String getEmrClusterId() {
        if (Boolean.TRUE.equals(useEmr)) {
            return emrCacheService.getClusterId();
        } else {
            return null;
        }
    }

}
