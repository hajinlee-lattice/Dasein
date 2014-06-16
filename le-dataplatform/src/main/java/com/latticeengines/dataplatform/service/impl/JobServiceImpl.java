package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.sqoop.Sqoop;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.MapReduceCustomizationService;
import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.dataplatform.service.YarnClientCustomizationService;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

@Component("jobService")
public class JobServiceImpl implements JobService, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(JobServiceImpl.class);
    private static final int MAX_TRIES = 60;
    private static final long APP_WAIT_TIME = 1000L;

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
    private Configuration hadoopConfiguration;

    @Autowired
    private ModelEntityMgr modelEntityMgr;
    
    @Autowired
    private ModelDefinitionEntityMgr modelDefinitionEntityMgr;    
    
    @Autowired
    private JobEntityMgr jobEntityMgr;
    
    @Autowired
    private MetadataService metadataService;

    @Autowired
    private AsyncTaskExecutor sqoopJobTaskExecutor;

    @Autowired
    private YarnService yarnService;
    
    @Autowired
    private JobNameService jobNameService;
    
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
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId submitJob(com.latticeengines.domain.exposed.dataplatform.Job job) {
        ApplicationId appId = submitYarnJob(job.getClient(), job.getAppMasterPropertiesObject(), job.getContainerPropertiesObject());
        job.setId(appId.toString());
        Model model = job.getModel();
                 
        ModelDefinition modelDefinition = model.getModelDefinition();
        // find the model def. already setup;  model def is expected to be pre-setup by user
        ModelDefinition predefinedModelDef = modelDefinitionEntityMgr.findByName(modelDefinition.getName());
        if (predefinedModelDef != null)  {
            // associate persisted model def with model.
            model.setModelDefinition(predefinedModelDef);
        } else {
            // TODO:  this should not be needed; since the way how it works is that model def is already created in persistence
            modelDefinitionEntityMgr.create(modelDefinition);
            model.setModelDefinition(modelDefinition);
        }  
        // create the model given the associated definition
        modelEntityMgr.createOrUpdate(model);
        
        return appId;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId resubmitPreemptedJob(com.latticeengines.domain.exposed.dataplatform.Job resubmitJob) {
        if (resubmitJob.getChildJobIdList().size() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Did not resubmit preempted job " + resubmitJob.getId() + ". Already resubmitted.");
            }
            return null;
        }
        String metadata = resubmitJob.getContainerPropertiesObject().getProperty(PythonContainerProperty.METADATA_CONTENTS.name());
        resubmitJob.getContainerPropertiesObject().setProperty(PythonContainerProperty.METADATA.name(), metadata);
        Long parentId = resubmitJob.getPid();
        resubmitJob.setId(null);
        resubmitJob.setParentJobId(parentId); 
        ApplicationId appId = submitJob(resubmitJob);
        log.info("Resubmitted " + parentId + " with " + resubmitJob.getId() + "to queue " + resubmitJob.getAppMasterPropertiesObject().getProperty(AppMasterProperty.QUEUE.name()) + ".");
        // find the parent job 
        com.latticeengines.domain.exposed.dataplatform.Job parentJob = new com.latticeengines.domain.exposed.dataplatform.Job();
        parentJob.setPid(parentId);
        parentJob = jobEntityMgr.findByKey(parentJob);
        parentJob.addChildJobId(resubmitJob.getId());
        jobEntityMgr.update(parentJob);
        return appId;
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
    
    private ApplicationId getAppIdFromName(String appName) {
        List<ApplicationReport> apps = defaultYarnClient.listApplications();
        for (ApplicationReport app : apps) {
            if (app.getName().equals(appName)) {
                return app.getApplicationId();
            }
        }
        return null;
    }

    @Override
    public ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols) {
        int numDefaultMappers = hadoopConfiguration.getInt("mapreduce.map.cpu.vcores", 4);
        return loadData(table, targetDir, creds, queue, customer, splitCols, numDefaultMappers);
    }

    @Override
    public ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, int numMappers) {
        
        final String jobName = jobNameService.createJobName(customer, "data-load");

        Future<Integer> future = loadAsync(table, targetDir, creds, queue, jobName, splitCols, numMappers);

        int tries = 0;
        ApplicationId appId = null;
        while (tries < MAX_TRIES) {
            try {
                Thread.sleep(APP_WAIT_TIME);
            } catch (InterruptedException e) {
                // do nothing
            	log.warn("Thread.sleep interrupted.", e);
            }
            appId = getAppIdFromName(jobName);
            if (appId != null) {
                return appId; 
            }
            tries++;
        }
        try {
            future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                log.error(e);
            }
        } catch (InterruptedException e) {
            log.error(e);
        }
        return getAppIdFromName(jobName);
    }

    private Future<Integer> loadAsync(final String table, final String targetDir, final DbCreds creds,
            final String queue, final String jobName, final List<String> splitCols, final int numMappers) {
        return sqoopJobTaskExecutor.submit(new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {

                return Sqoop.runTool(new String[] { //
                        "import", //
                        "-Dmapred.job.queue.name=" + queue, //
                        "--connect", //
                        metadataService.getJdbcConnectionUrl(creds), //
                        "--m", //
                        Integer.toString(numMappers), // 
                        "--table", //
                        table, //
                        "--as-avrodatafile",
                        "--compress", //
                        "--mapreduce-job-name", //
                        jobName, //
                        "--split-by", //
                        StringUtils.join(splitCols, ","), //
                        "--target-dir", //
                        targetDir }, yarnConfiguration);

            }
        });
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        com.latticeengines.domain.exposed.dataplatform.Job leafJob = getLeafJob(applicationId);
        JobStatus jobStatus = new JobStatus();
        jobStatus.setId(applicationId);
        if (leafJob != null) {
            applicationId = leafJob.getId();
            jobStatus.setId(applicationId);
            String classifierStr = (String) leafJob.getContainerPropertiesObject().get(
                    PythonContainerProperty.METADATA_CONTENTS.name());
            if (classifierStr != null) {
                Classifier classifier = JsonUtils.deserialize(classifierStr, Classifier.class);
                if (classifier != null) {
                    String[] tokens = StringUtils.split(applicationId, "_");
                    String folder = StringUtils.join(new String[] { tokens[1], tokens[2] }, "_");
                    jobStatus.setResultDirectory(classifier.getModelHdfsDir() + "/" + folder);
                }
            }
        }

        ApplicationReport appReport = defaultYarnClient.getApplicationReport(YarnUtils.getApplicationIdFromString(applicationId));
        if (appReport != null) {
            jobStatus.setState(appReport.getFinalApplicationStatus());
            jobStatus.setDiagnostics(appReport.getDiagnostics());
            jobStatus.setFinishTime(appReport.getFinishTime());
            jobStatus.setProgress(appReport.getProgress());
            jobStatus.setStartTime(appReport.getStartTime());
            jobStatus.setTrackingUrl(appReport.getTrackingUrl());
        }

        return jobStatus;
    }

    private com.latticeengines.domain.exposed.dataplatform.Job getLeafJob(String applicationId) {
        com.latticeengines.domain.exposed.dataplatform.Job job = jobEntityMgr.findByObjectId(applicationId); /// jobEntityMgr.getById(applicationId);
        
        if (job != null) {
            List<String> childIds = job.getChildJobIdList();
            for (String jobId : childIds) {
                return getLeafJob(jobId);
            }
        }
        return job;

    }
}
