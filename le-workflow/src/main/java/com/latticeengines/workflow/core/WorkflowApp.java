package com.latticeengines.workflow.core;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSBatchConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowProperty;
import com.latticeengines.workflow.exposed.service.WorkflowService;

public class WorkflowApp {
    private static final Logger log = LoggerFactory.getLogger(WorkflowApp.class);
    
    private static final String[] contexts = { "workflow-context.xml", "common-properties-context.xml" };

    public static void main(String[] args) {

        String workflowConfig = System.getenv(WorkflowProperty.WORKFLOWCONFIG);
        String workflowConfigClass = System.getenv(WorkflowProperty.WORKFLOWCONFIGCLASS);
        if (StringUtils.isNotBlank(workflowConfig) && StringUtils.isNotBlank(workflowConfigClass)) {
            runWorkflowInAws(workflowConfig, workflowConfigClass);
            return;
        }
        String stepflowConfig = System.getenv(WorkflowProperty.STEPFLOWCONFIG);
        String stepflowConfigClass = System.getenv(WorkflowProperty.STEPFLOWCONFIGCLASS);
        if (StringUtils.isNotBlank(stepflowConfig) && StringUtils.isNotBlank(stepflowConfigClass)) {
            runStepflowInAws(stepflowConfig, stepflowConfigClass);
            return;
        }
        log.info("There's no workflow to run in Aws.");
        
        runWorkFlow("dlOrchestrationWorkflow");

    }

    private static void runWorkFlow(String workFlowName) {

        try (ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(contexts)) {
            WorkflowService workflowService = (WorkflowService) context.getBean("workflowService");
            WorkflowConfiguration config = new WorkflowConfiguration();
            config.setWorkflowName(workFlowName);
            WorkflowExecutionId workflowId = workflowService.start(config);
            BatchStatus status = workflowService.waitForCompletion(workflowId).getStatus();

            log.info("Exit Status : " + status);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void runStepflowInAws(String stepflowConfig, String stepflowConfigClass) {

        AWSBatchConfiguration config = null;
        try (ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(contexts)) {

            Class<?> clazz = Class.forName(stepflowConfigClass);
            config = (AWSBatchConfiguration) JsonUtils.deserialize(stepflowConfig, clazz);
            log.info("Starting step flow bean name = " + config.getBeanName());

            BaseAwsBatchStep step = context.getBean(config.getBeanName(), BaseAwsBatchStep.class);
            step.setConfiguration(config);
            step.execute();
            log.info("Finished step flow bean name =" + config.getBeanName());

        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            String step = config != null ? config.getBeanName() : "";
            throw new RuntimeException("Faied to run step=" + step);
        }
    }

    private static void runWorkflowInAws(String workflowConfig, String workflowConfigClass) {

        log.info("Starting workflow= ");
        try (ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(contexts)) {
            WorkflowConfiguration config = null;
            Class<?> clazz = Class.forName(workflowConfigClass);
            config = (WorkflowConfiguration) JsonUtils.deserialize(workflowConfig, clazz);
            log.info("Starting workflow= " + config.getWorkflowName());

            WorkflowService workflowService = (WorkflowService) context.getBean("workflowService");
            WorkflowExecutionId workflowId = workflowService.start(config);
            BatchStatus status = workflowService.waitForCompletion(workflowId).getStatus();

            log.info("Finished workflow=" + config.getWorkflowName() + " Exit Status : " + status);
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
    }
}
