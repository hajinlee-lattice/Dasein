package com.latticeengines.workflow.core;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.ProcessResult;

import com.latticeengines.aws.batch.BatchService;
import com.latticeengines.aws.batch.JobRequest;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowProperty;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public abstract class BaseAwsPythonBatchStep<T extends AWSPythonBatchConfiguration> extends AbstractStep<T>
        implements ApplicationContextAware {
    private static final String AWS_PYTHON_BATCH_CONFIGURATION = "AWS_PYTHON_BATCH_CONFIGURATION";

    private static Logger log = LoggerFactory.getLogger(BaseAwsPythonBatchStep.class);

    @Value("${hadoop.fs.web.defaultFS}")
    String webHdfs;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Inject
    private VersionManager versionManager;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private EMRCacheService emrCacheService;

    protected ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Autowired
    private BatchService batchService;

    private String jobName = null;
    private AWSPythonBatchConfiguration config = null;

    @Override
    public void execute() {
        try {
            log.info("Inside BaseAwsPythonBatchStep execute().");
            if (configuration.isSubmission()) {
                config = getObjectFromContext(AWS_PYTHON_BATCH_CONFIGURATION, AWSPythonBatchConfiguration.class);
                if (config != null) {
                    log.info("There's batch job running, jobId=", config.getJobId());
                    return;
                }
                config = configuration;
                setupConfig(config);
                if (config.isRunInAws()) {
                    executeInAws();
                }
                putObjectInContext(AWS_PYTHON_BATCH_CONFIGURATION, config);
                return;
            }

            config = getObjectFromContext(AWS_PYTHON_BATCH_CONFIGURATION, AWSPythonBatchConfiguration.class);
            if (config != null && config.isRunInAws()) {
                waitForCompletion();
                afterComplete(config);
                return;
            }
            boolean result = submitAndWaitForCompletion();
            if (!result) {
                throw new RuntimeException("Aws Batch job failed!");
            }
        } catch (Exception ex) {
            log.error("Failed to run Python App!", ex);
            throw new RuntimeException("Failed to generate APS table!");
        }
    }

    private boolean submitAndWaitForCompletion() {
        if (config == null) {
            config = configuration;
            setupConfig(config);
        }
        if (CollectionUtils.isEmpty(config.getInputPaths())) {
            log.warn("No input path was generated.");
            return false;
        }

        boolean result;
        if (config.isRunInAws()) {
            executeInAws();
            result = waitForCompletion();
            log.info("Submitted Aws Batch Job, result=" + result);
        } else {
            result = executeInline();
            log.info("Submitted Inline Job, result=" + result);
        }
        afterComplete(config);
        return result;

    }

    public boolean waitForCompletion() {
        String batchJobId = config.getJobId();
        boolean result = batchService.waitForCompletion(batchJobId, 1000 * 60 * 300L);
        log.info("Finished waiting for Aws Batch Job, result=" + result);
        return result;
    }

    private boolean executeInAws() {
        JobRequest jobRequest = createJobRequest();
        String batchJobId = batchService.submitJob(jobRequest);
        config.setJobId(batchJobId);
        boolean result = StringUtils.isNotBlank(batchJobId);
        log.info("Job name=" + jobName + " Job id=" + batchJobId + " is submitted=" + result);
        return result;
    }

    private JobRequest createJobRequest() {

        JobRequest jobRequest = new JobRequest();
        jobName = config.getCustomerSpace().getTenantId() + "_Aws_Python_" + name();
        jobName = jobName.replaceAll(" ", "_");
        log.info("Job name=" + jobName);
        jobRequest.setJobName(jobName);

        Map<String, String> envs = getRuntimeEnvs();
        jobRequest.setEnvs(envs);
        return jobRequest;
    }

    protected Map<String, String> getRuntimeEnvs() {
        Map<String, String> envs = new HashMap<>();
        envs.put(WorkflowProperty.STEPFLOWCONFIG, config.toString());
        envs.put("CONDA_ENV", getCondaEnv());
        envs.put("PYTHON_APP", getPythonScript());

        if (Boolean.TRUE.equals(useEmr)) {
            envs.put("SHDP_HD_FSWEB", emrCacheService.getWebHdfsUrl());
        } else {
            envs.put("SHDP_HD_FSWEB", webHdfs);
        }

        return envs;
    }

    protected File getPythonWorkspace() {
        return new File("python_workspace");
    }

    protected void executePythonCommand(Map<String, String> envs) {
        try {
            ProcessExecutor executor = new ProcessExecutor().directory(getPythonWorkspace());
            executor = executor.environment(envs) //
                    .command("bash", "pythonlauncher.sh", getCondaEnv(), getPythonScript()).redirectOutput(System.out);
            ProcessResult result = executor.execute();
            int exit = result.getExitValue();
            log.info("Exit code of python command: " + exit);
            if (exit != 0) {
                throw new RuntimeException("Python program did not exit with code 0, please check error above.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute python command.", e);
        }
    }

    protected abstract String getCondaEnv();

    protected abstract String getPythonScript();

    protected abstract void localizePythonScripts();

    private boolean executeInline() {
        localizePythonScripts();
        config.setRunInAws(false);
        Map<String, String> envs = getRuntimeEnvs();
        executePythonCommand(envs);
        return true;
    }

    protected void setupConfig(AWSPythonBatchConfiguration config) {
    }

    protected void afterComplete(AWSPythonBatchConfiguration config) {
    }

    protected String getScriptDirInHdfs() {
        String artifactVersion = versionManager.getCurrentVersionInStack(stackName);
        String scriptDir = StringUtils.isEmpty(artifactVersion) ? "/app/dataplatform/scripts"
                : "/app/" + artifactVersion + "/dataplatform/scripts";
        log.info("Using python script dir = " + scriptDir);
        return scriptDir;
    }
}
