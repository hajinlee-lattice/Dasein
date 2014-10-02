package com.latticeengines.dataplatform.service.modeling.impl;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.sqoop.LedpSqoop;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.dataplatform.service.impl.JobServiceImpl;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

@Component("modelingJobService")
public class ModelingJobServiceImpl extends JobServiceImpl implements ModelingJobService {

    @Autowired
    private AsyncTaskExecutor sqoopJobTaskExecutor;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private ModelEntityMgr modelEntityMgr;

    @Autowired
    private ModelDefinitionEntityMgr modelDefinitionEntityMgr;

    @Autowired
    private JobNameService jobNameService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

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
                if (e.getCause().getMessage().contains("No columns to generate for ClassWriter")) {
                    throw new LedpException(LedpCode.LEDP_12008, new String[] { table });
                } else {
                    throw (RuntimeException) e.getCause();
                }
            } else {
                log.error(e);
            }
        } catch (InterruptedException e) {
            log.error(e);
        }
        // Final try
        appId = getAppIdFromName(jobName);
        if (appId == null) {
            throw new LedpException(LedpCode.LEDP_12002, new String[] { jobName });
        }
        return appId;
    }

    private Future<Integer> loadAsync(final String table, final String targetDir, final DbCreds creds,
            final String queue, final String jobName, final List<String> splitCols, final int numMappers) {
        return sqoopJobTaskExecutor.submit(new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {

                return LedpSqoop.runTool(new String[] { //
                        "import", //
                                "-Dmapred.job.queue.name=" + queue, //
                                "--connect", //
                                metadataService.getJdbcConnectionUrl(creds), //
                                "--m", //
                                Integer.toString(numMappers), //
                                "--table", //
                                table, //
                                "--as-avrodatafile", "--compress", //
                                "--mapreduce-job-name", //
                                jobName, //
                                "--split-by", //
                                StringUtils.join(splitCols, ","), //
                                "--target-dir", //
                                targetDir }, new Configuration(yarnConfiguration));

            }
        });
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId submitJob(com.latticeengines.domain.exposed.dataplatform.Job job) {
        ApplicationId appId = super.submitJob(job);
        job.setId(appId.toString());
        Model model = job.getModel();

        ModelDefinition modelDefinition = model.getModelDefinition();
        // find the model def. already setup; model def is expected to be
        // pre-setup by user
        ModelDefinition predefinedModelDef = modelDefinitionEntityMgr.findByName(modelDefinition.getName());
        if (predefinedModelDef != null) {
            // associate persisted model def with model.
            model.setModelDefinition(predefinedModelDef);
        } else {
            // TODO: this should not be needed; since the way how it works is
            // that model def is already created in persistence
            modelDefinitionEntityMgr.create(modelDefinition);
            model.setModelDefinition(modelDefinition);
        }
        // create the model given the associated definition
        modelEntityMgr.createOrUpdate(model);

        return appId;
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        com.latticeengines.domain.exposed.dataplatform.Job leafJob = getLeafJob(applicationId);
        JobStatus jobStatus = new JobStatus();
        if (leafJob != null) {
            applicationId = leafJob.getId();
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
        super.setJobStatus(jobStatus, applicationId);
        return jobStatus;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ApplicationId resubmitPreemptedJob(com.latticeengines.domain.exposed.dataplatform.Job resubmitJob) {
        if (resubmitJob.getChildJobIdList().size() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Did not resubmit preempted job " + resubmitJob.getId() + ". Already resubmitted.");
            }
            return null;
        }
        Long parentId = resubmitJob.getPid();
        String metadata = resubmitJob.getContainerPropertiesObject().getProperty(
                PythonContainerProperty.METADATA_CONTENTS.name());

        com.latticeengines.domain.exposed.dataplatform.Job newJob = new com.latticeengines.domain.exposed.dataplatform.Job();
        newJob.setParentJobId(parentId);
        newJob.setModel(resubmitJob.getModel());
        newJob.setClient(resubmitJob.getClient());
        newJob.setAppMasterPropertiesObject(resubmitJob.getAppMasterPropertiesObject());
        newJob.setContainerPropertiesObject(resubmitJob.getContainerPropertiesObject());
        newJob.getContainerPropertiesObject().setProperty(PythonContainerProperty.METADATA.name(), metadata);
        // submit job to yarn and persist metadata
        ApplicationId appId = submitJob(newJob);
        jobEntityMgr.createOrUpdate(newJob);
        if (log.isInfoEnabled()) {
            log.info("Resubmitted job pid(" + parentId + ") and received new appId(" + newJob.getId() + ") in queue "
                    + newJob.getAppMasterPropertiesObject().getProperty(AppMasterProperty.QUEUE.name()) + ".");
        }
        resubmitJob.addChildJobId(newJob.getId());
        jobEntityMgr.update(resubmitJob);

        return appId;
    }

    @Override
    public JobStatus getJobStatus(String applicationId, String hdfs) throws Exception {
        com.latticeengines.domain.exposed.dataplatform.Job leafJob = getLeafJob(applicationId);
        JobStatus jobStatus = new JobStatus();
        if (leafJob != null) {
            applicationId = leafJob.getId();
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
        super.setJobStatus(jobStatus, applicationId, hdfs);
        return jobStatus;
    }

    protected com.latticeengines.domain.exposed.dataplatform.Job getLeafJob(String applicationId) {
        com.latticeengines.domain.exposed.dataplatform.Job job = jobEntityMgr.findByObjectId(applicationId); // /
        // jobEntityMgr.getById(applicationId);

        while (job != null && job.getChildJobIdList().size() > 0) {
            applicationId = job.getChildJobIdList().get(0);
            job = jobEntityMgr.findByObjectId(applicationId);
        }
        return job;
    }
}
