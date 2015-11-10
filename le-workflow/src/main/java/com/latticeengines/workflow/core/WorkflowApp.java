package com.latticeengines.workflow.core;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflow.exposed.service.WorkflowService;

// TODO bernard turn this into a real CLI
public class WorkflowApp {

    private static final Log log = LogFactory.getLog(WorkflowApp.class);

    public static void main(String[] args) {
        String[] springConfig = { "workflow-context.xml", "workflow-properties-context.xml" };

        try (ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(springConfig)) {
            WorkflowService workflowService = (WorkflowService) context.getBean("workflowService");

            WorkflowExecutionId workflowId = workflowService.start("dlOrchestrationWorkflow", null);
            BatchStatus status = workflowService.waitForCompletion(workflowId).getStatus();

            log.info("Exit Status : " + status);
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
        }

    }
}