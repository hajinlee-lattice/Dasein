package com.latticeengines.workflow.library;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.support.ClassPathXmlApplicationContext;

// TODO turn this into a real CLI
public class WorkflowApp {

    private static final Log log = LogFactory.getLog(WorkflowApp.class);

    public static void main(String[] args) {
        String[] springConfig = { "workflow-context.xml" };

        try (ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(springConfig)) {
            JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
            JobRegistry jobRegistry = (JobRegistry) context.getBean("jobRegistry");

            Job job = jobRegistry.getJob("DLOrchestrationWorkflow");
            JobExecution execution = jobLauncher.run(job, new JobParameters());
            log.info("Exit Status : " + execution.getStatus());
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
        }

    }
}