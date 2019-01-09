package com.latticeengines.yarn.exposed.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.batch.AWSBatchConfig;
import com.latticeengines.aws.batch.BatchService;
import com.latticeengines.aws.batch.JobRequest;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.aws.AwsApplicationId;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonContainerProperty;
import com.latticeengines.yarn.exposed.service.AwsBatchJobService;
import com.latticeengines.yarn.exposed.service.YarnClientCustomizationService;

@Component("awsBatchjobService")
public class AwsBatchJobServiceImpl implements AwsBatchJobService {

    private static final String LAUNCHER_PY = "launcher.py";

    private static final String APP_PY = "app.py";

    protected static final Logger log = LoggerFactory.getLogger(AwsBatchJobServiceImpl.class);

    @Autowired
    private BatchService batchService;

    @Autowired
    private JobServiceHelper jobServiceHelper;

    @Value("${hadoop.fs.web.defaultFS}")
    String webHdfs;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    @Value("${dataplatform.yarn.job.runtime.config}")
    private String runtimeConfig;

    @Value("${dataplatform.model.aws.batch.local.enabled}")
    private boolean batchLocal;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnClientCustomizationService awsBatchCustomizationService;

    public ApplicationId submitAwsBatchJob(com.latticeengines.domain.exposed.dataplatform.Job job) {
        awsBatchCustomizationService.validate(null, job.getClient(), job.getAppMasterPropertiesObject(),
                job.getContainerPropertiesObject());
        awsBatchCustomizationService.addCustomizations(null, job.getClient(), job.getAppMasterPropertiesObject(),
                job.getContainerPropertiesObject());
        try {
            if (batchLocal) {
                return runAwsBatchLocal(job);
            }
            String batchJobId = executeInAwsBatch(job);
            AwsApplicationId appId = new AwsApplicationId();
            appId.setJobId(batchJobId);
            return appId;
        } finally {
            awsBatchCustomizationService.finalize(job.getClient(), job.getAppMasterPropertiesObject(),
                    job.getContainerPropertiesObject());
        }
    }

    @Override
    public JobStatus getAwsBatchJobStatus(String jobId) {
        JobStatus status = new JobStatus();
        status.setId(jobId);
        if (batchLocal) {
            status.setStatus(FinalApplicationStatus.SUCCEEDED);
            return status;
        }
        jobId = AwsApplicationId.getAwsBatchJob(jobId);
        String batchStatus = batchService.getJobStatus(jobId);
        log.info("Got Aws batch job status = " + batchStatus);
        status.setStatus(FinalApplicationStatus.UNDEFINED);
        if ("SUCCEEDED".equals(batchStatus)) {
            status.setStatus(FinalApplicationStatus.SUCCEEDED);
        } else if ("FAILED".equals(batchStatus)) {
            status.setStatus(FinalApplicationStatus.FAILED);
        }
        return status;
    }

    private String executeInAwsBatch(com.latticeengines.domain.exposed.dataplatform.Job job) {
        JobRequest jobRequest = createAwsBatchJobRequest(job);
        String batchJobId = batchService.submitJob(jobRequest);
        boolean result = StringUtils.isNotBlank(batchJobId);
        log.info("Job name=" + jobRequest.getJobName() + " Job id=" + batchJobId + " is submitted=" + result);
        return batchJobId;
    }

    private JobRequest createAwsBatchJobRequest(com.latticeengines.domain.exposed.dataplatform.Job job) {
        Properties appMasterProperties = job.getAppMasterPropertiesObject();
        Properties containerProperties = job.getContainerPropertiesObject();

        JobRequest jobRequest = new JobRequest();
        String jobName = jobServiceHelper.appName(job.getAppMasterPropertiesObject(), job.getClient());
        jobName = jobName.replaceAll("[^0-9a-zA-Z-_]", "_");
        jobName = jobName.substring(0, jobName.length());

        jobRequest.setJobName(jobName);
        jobRequest.setMemory(getResourceConfig(containerProperties, ContainerProperty.MEMORY.name()));
        jobRequest.setCpus(getResourceConfig(containerProperties, ContainerProperty.VIRTUALCORES.name()));
        Map<String, String> envs = getAwsBatchRuntimeEnvs(job, appMasterProperties, containerProperties, null);
        jobRequest.setEnvs(envs);
        return jobRequest;
    }

    private Integer getResourceConfig(Properties containerProperties, String key) {
        String value = containerProperties.getProperty(key);
        try {
            return Integer.parseInt(value);
        } catch (Exception ex) {
            log.warn("Failed to set " + key + " value=" + value);
            return null;
        }
    }

    private Map<String, String> getAwsBatchRuntimeEnvs(com.latticeengines.domain.exposed.dataplatform.Job job,
            Properties appMasterProperties, Properties containerProperties, AwsApplicationId applicationId) {

        Map<String, String> envs = new HashMap<>();
        AWSBatchConfig config = new AWSBatchConfig();
        config.setInputPaths(getInputPaths(containerProperties));

        envs.put("PYTHON_APP_CONFIG", config.toString());
        envs.put("PYTHON_APP_LAUNCH", LAUNCHER_PY);
        envs.put("PYTHON_APP_ARGS",
                "metadata.json " + containerProperties.getProperty(ContainerProperty.RUNTIME_CONFIG.name()));
        envs.put("PYTHON_APP", APP_PY);
        if (applicationId != null) {
            envs.put("AWS_BATCH_JOB_ID", applicationId.toString());
        }

        envs.put(PythonContainerProperty.CONDA_ENV.name(),
                containerProperties.getProperty(PythonContainerProperty.CONDA_ENV.name()));
        envs.put(PythonContainerProperty.SHDP_HD_FSWEB.name(),
                containerProperties.getProperty(PythonContainerProperty.WEBHDFS_URL.name()));
        envs.put(PythonContainerProperty.PYTHONPATH.name(),
                containerProperties.getProperty(PythonContainerProperty.PYTHONPATH.name()));
        envs.put(PythonContainerProperty.PYTHONIOENCODING.name(), "UTF-8");
        envs.put(PythonContainerProperty.DEBUG.name(), "false");

        return envs;
    }

    private List<String> getInputPaths(Properties containerProperties) {
        List<String> paths = new ArrayList<>();
        paths.add(jobServiceHelper.getJobDir(containerProperties) + "/*");
        String version = containerProperties.getProperty(PythonContainerProperty.VERSION.name());
        paths.add("/app/" + version + "/dataplatform/scripts/*");
        paths.add("/app/" + version + "/dataplatform/scripts/leframework.tar.gz");

        paths.add(containerProperties.getProperty(PythonContainerProperty.TRAINING.name()));
        paths.add(containerProperties.getProperty(PythonContainerProperty.TEST.name()));
        paths.add(containerProperties.getProperty(PythonContainerProperty.PYTHONSCRIPT.name()));
        paths.add(containerProperties.getProperty(PythonContainerProperty.PYTHONPIPELINESCRIPT.name()));
        paths.add(containerProperties.getProperty(PythonContainerProperty.PYTHONPIPELINELIBFQDN.name()));
        paths.add(containerProperties.getProperty(PythonContainerProperty.SCHEMA.name()));
        paths.add(containerProperties.getProperty(PythonContainerProperty.DATAPROFILE.name()));
        paths.add(containerProperties.getProperty(PythonContainerProperty.CONFIGMETADATA.name()));
        paths.add(containerProperties.getProperty(PythonContainerProperty.PIPELINEDRIVER.name()));

        return paths;
    }

    /* run batch on localhost */
    private ApplicationId runAwsBatchLocal(com.latticeengines.domain.exposed.dataplatform.Job job) {
        localizePythonScripts(job);
        AwsApplicationId applicationId = new AwsApplicationId();
        applicationId.setJobId(UUID.randomUUID().toString());
        Map<String, String> envs = getAwsBatchRuntimeEnvs(job, job.getAppMasterPropertiesObject(),
                job.getContainerPropertiesObject(), applicationId);
        jobServiceHelper.executePythonCommand(envs,
                job.getContainerPropertiesObject().getProperty(PythonContainerProperty.CONDA_ENV.name()),
                getPythonWorkspace(), APP_PY);
        return applicationId;
    }

    private void localizePythonScripts(com.latticeengines.domain.exposed.dataplatform.Job job) {
        try {
            String scriptDir = getScriptDirInHdfs(job);
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, scriptDir + "/webhdfs.py",
                    getPythonWorkspace().getPath() + "/webhdfs.py");
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, scriptDir + "/pythonlauncher.sh",
                    getPythonWorkspace().getPath() + "/pythonlauncher.sh");
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, scriptDir + "/app.py",
                    getPythonWorkspace().getPath() + "/app.py");
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, scriptDir + "/apploader.py",
                    getPythonWorkspace().getPath() + "/apploader.py");
        } catch (IOException e) {
            throw new RuntimeException("Failed to localize python scripts", e);
        }
    }

    public String getScriptDirInHdfs(com.latticeengines.domain.exposed.dataplatform.Job job) {
        Properties containerProperties = job.getContainerPropertiesObject();
        String artifactVersion = containerProperties.getProperty(PythonContainerProperty.VERSION.name());
        String scriptDir = StringUtils.isEmpty(artifactVersion) ? "/app/dataplatform/scripts"
                : "/app/" + artifactVersion + "/dataplatform/scripts";
        log.info("Using python script dir = " + scriptDir);
        return scriptDir;
    }

    private File getPythonWorkspace() {
        return new File("python_model_workspace");
    }

}
