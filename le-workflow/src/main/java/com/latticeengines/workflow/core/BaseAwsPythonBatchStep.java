package com.latticeengines.workflow.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.latticeengines.aws.batch.BatchService;
import com.latticeengines.aws.batch.JobRequest;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowProperty;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public abstract class BaseAwsPythonBatchStep<T extends AWSPythonBatchConfiguration> extends AbstractStep<T>
        implements ApplicationContextAware {
    private static Log log = LogFactory.getLog(BaseAwsPythonBatchStep.class);

    @Value("${hadoop.fs.web.defaultFS}")
    String webHdfs;

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
            config = getConfiguration();
            setupConfig(config);
            if (CollectionUtils.isEmpty(config.getInputPaths())) {
                log.info("There's no input paths generated for Aps!");
                return;
            }
            log.info("Inside BaseAwsPythonBatchStep execute(), runInAws=" + config.isRunInAws());
            boolean result = false;
            if (config.isRunInAws()) {
                result = executeInAws();
            } else {
                result = executeInline();
            }
            if (result) {
                afterComplete(config);
            }
        } catch (Exception ex) {
            log.error("Failed to run Python App!", ex);
        }
    }

    private boolean executeInAws() {
        JobRequest jobRequest = createJobRequest();
        String jobId = batchService.submitJob(jobRequest);
        boolean result = batchService.waitForCompletion(jobId, 1000 * 60 * 300L);
        log.info("Job name=" + jobName + " Job id=" + jobId + " is successful=" + result);
        return result;
    }

    private JobRequest createJobRequest() {

        JobRequest jobRequest = new JobRequest();
        jobName = config.getCustomerSpace().getTenantId() + "_Aws_Python_" + name();
        jobName = jobName.replaceAll(" ", "_");
        log.info("Job name=" + jobName);
        jobRequest.setJobName(jobName);
        jobRequest.setJobDefinition("AWS-Python-Workflow-Job-Definition");
        jobRequest.setJobQueue("AWS-Python-Workflow-Job-Queue");

        Map<String, String> envs = new HashMap<>();
        config.setRunInAws(false);
        envs.put(WorkflowProperty.STEPFLOWCONFIG, config.toString());
        envs.put("CONDA_ENV", getAcondaEnv());
        envs.put("PYTHON_APP", getPythonScript());
         envs.put("SHDP_HD_FSWEB", webHdfs);
        // envs.put("SHDP_HD_FSWEB",
        // "http://webhdfs.lattice.local:14000/webhdfs/v1");

        jobRequest.setEnvs(envs);
        return jobRequest;
    }

    protected abstract String getAcondaEnv();

    protected abstract String getPythonScript();

    protected boolean executeInline() {
        return true;
    }

    protected void setupConfig(AWSPythonBatchConfiguration config) {
    }

    protected void afterComplete(AWSPythonBatchConfiguration config) {
    }

}
