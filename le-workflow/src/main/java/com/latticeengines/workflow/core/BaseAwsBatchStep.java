package com.latticeengines.workflow.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.latticeengines.aws.batch.BatchService;
import com.latticeengines.aws.batch.JobRequest;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSBatchConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowProperty;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public abstract class BaseAwsBatchStep<T extends AWSBatchConfiguration> extends AbstractStep<T> implements
        ApplicationContextAware {
    private static Log log = LogFactory.getLog(BaseAwsBatchStep.class);

    protected ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Autowired
    private BatchService batchService;

    private String jobName = null;
    private AWSBatchConfiguration config = null;

    protected abstract void executeInline();

    @Override
    public void execute() {
        config = getConfiguration();
        log.info("Inside BaseAwsBatchStep execute(), runInAws=" + config.isRunInAws());
        if (config.isRunInAws()) {
            executeInAws();
        } else {
            executeInline();
        }
    }

    private void executeInAws() {
        JobRequest jobRequest = createJobRequest();
        String jobId = batchService.submitJob(jobRequest);
        boolean result = batchService.waitForCompletion(jobId, 1000 * 60 * 300L);
        log.info("Job name=" + jobName + " Job id=" + jobId + " is successful=" + result);
    }

    private JobRequest createJobRequest() {

        JobRequest jobRequest = new JobRequest();
        jobName = config.getCustomerSpace().getTenantId() + "_Aws_" + name();
        jobName = jobName.replaceAll(" ", "_");
        log.info("Job name=" + jobName);
        jobRequest.setJobName(jobName);
        jobRequest.setJobDefinition("AWS-Workflow-Job-Definition");
        jobRequest.setJobQueue("AWS-Workflow-Job-Queue");

        Integer memory = config.getContainerMemoryMB();
        if (memory == null) {
            memory = 2048;
        }
        log.info("Set container memory to " + memory + " mb.");
        jobRequest.setMemory(memory);
        jobRequest.setCpus(1);
        Map<String, String> envs = new HashMap<>();
        config.setBeanName(name());
        config.setRunInAws(false);
        envs.put(WorkflowProperty.STEPFLOWCONFIG, config.toString());
        envs.put(WorkflowProperty.STEPFLOWCONFIGCLASS, config.getClass().getName());

        jobRequest.setEnvs(envs);
        return jobRequest;
    }

}
