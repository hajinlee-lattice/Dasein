package org.springframework.yarn.batch.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobExecutionNotFailedException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Component;
import org.springframework.util.PatternMatchUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.batch.event.JobExecutionEvent;
import org.springframework.yarn.batch.support.YarnBatchProperties.JobProperties;

@Component
public class YarnJobLauncher implements ApplicationEventPublisherAware {

    private static final Logger log = LoggerFactory.getLogger(YarnJobLauncher.class);

    private JobParametersConverter converter = new DefaultJobParametersConverter();

    private JobLauncher jobLauncher;

    private JobRegistry jobRegistry;

    private JobExplorer jobExplorer;

    private YarnBatchProperties yarnBatchProperties;

    private Collection<Job> jobs = Collections.emptySet();

    private String jobName;

    private ApplicationEventPublisher publisher;

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    /**
     * Run the jobs.
     *
     * @param args the args
     * @throws JobExecutionException the job execution exception
     */
    public void run(String... args) throws JobExecutionException {
        log.info("Running default command line with: " + Arrays.asList(args));
        launchJobFromProperties(StringUtils.splitArrayElementsIntoProperties(args, "="));
    }

    /**
     * Run the jobs.
     *
     * @param properties the job properties
     * @throws JobExecutionException the job execution exception
     */
    public void run(Properties properties) throws JobExecutionException {
        launchJobFromProperties(properties);
    }

    /**
     * Sets the enabled job name. Moreover this name can also be a simple
     * pattern supported by
     * {@link PatternMatchUtils#simpleMatch(String, String)} and multiple
     * patterns can be matched if delimited by a comma.
     *
     * @param jobName the new job name
     */
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    /**
     * Gets the enabled job name.
     *
     * @return the job name
     * @see #setJobName(String)
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Sets the job launcher.
     *
     * @param jobLauncher the new job launcher
     */
    @Inject
    public void setJobLauncher(JobLauncher jobLauncher) {
        this.jobLauncher = jobLauncher;
    }

    /**
     * Gets the job launcher.
     *
     * @return the job launcher
     */
    public JobLauncher getJobLauncher() {
        return jobLauncher;
    }

    @Inject
    public void setJobRegistry(JobRegistry jobRegistry) {
        this.jobRegistry = jobRegistry;
    }

    public JobRegistry getJobRegistry() {
        return jobRegistry;
    }

    @Inject
    public void setJobParametersConverter(JobParametersConverter converter) {
        this.converter = converter;
    }

    @Inject
    public void setJobExplorer(JobExplorer jobExplorer) {
        this.jobExplorer = jobExplorer;
    }

    public JobExplorer getJobExplorer() {
        return jobExplorer;
    }

    /**
     * Sets the jobs.
     *
     * @param jobs the new jobs
     */
    @Inject
    public void setJobs(Collection<Job> jobs) {
        this.jobs = jobs;
    }

    /**
     * Gets the jobs.
     *
     * @return the jobs
     */
    public Collection<Job> getJobs() {
        return jobs;
    }

    @Inject
    public void setYarnBatchProperties(YarnBatchProperties yarnBatchProperties) {
        this.yarnBatchProperties = yarnBatchProperties;
    }

    /**
     * Launch jobs using {@link Properties}.
     *
     * @param properties the properties
     * @throws JobExecutionException the job execution exception
     */
    protected void launchJobFromProperties(Properties properties) throws JobExecutionException {
        JobParameters jobParameters = this.converter.getJobParameters(properties);
        executeRegisteredJobs(jobParameters);
        executeLocalJobs(jobParameters);
    }

    private void executeRegisteredJobs(JobParameters jobParameters) throws JobExecutionException {
        if (this.jobRegistry == null) {
            log.info("No jobRegistry defined, using MapJobRegistry as default");
            this.jobRegistry = new MapJobRegistry();
        }
        if (this.jobRegistry != null && StringUtils.hasText(this.jobName)) {
            for (String name : this.jobRegistry.getJobNames()) {
                if (StringUtils.hasText(this.jobName) && jobMatches(this.jobName, name)) {
                    Job job = this.jobRegistry.getJob(name);
                    executeJob(job, jobParameters);
                } else {
                    log.debug("Skipped registered job: " + name);
                }

            }
        }
    }

    private void executeLocalJobs(JobParameters jobParameters) throws JobExecutionException {
        if (jobs.size() == 0) {
            log.info("No local jobs defined");
        }
        for (Job job : jobs) {
            if (StringUtils.hasText(this.jobName) && jobMatches(this.jobName, job.getName())) {
                log.debug("Executing local job: " + job.getName());
                executeJob(job, jobParameters);
            } else {
                log.debug("Skipped local job: " + job.getName());
            }
        }
    }

    private static boolean jobMatches(String patterns, String name) {
        for (String pattern : StringUtils.commaDelimitedListToStringArray(patterns)) {
            if (PatternMatchUtils.simpleMatch(pattern, name)) {
                return true;
            }
        }
        return false;
    }

    protected void executeJob(Job job, JobParameters jobParameters) throws JobExecutionException {
        String jobIdentifier = job.getName();
        JobProperties jobProperties = yarnBatchProperties != null ? yarnBatchProperties.getJobProperties(jobIdentifier) : null;
        boolean restart = false;

        // re-create by adding props from a boot JobProperties
        if (jobProperties != null && jobProperties.getParameters() != null) {
            log.info("Job parameters from boot properties, parameters" + jobProperties.getParameters());
            Properties tmpProperties = new Properties();
            Map<String, Object> tmpParameters = jobProperties.getParameters();
            tmpProperties.putAll(tmpParameters);
            JobParameters tmpJobParameters = this.converter.getJobParameters(tmpProperties);
            Map<String, JobParameter> map1 = new HashMap<String, JobParameter>(tmpJobParameters.getParameters());
            map1.putAll(jobParameters.getParameters());
            jobParameters = new JobParameters(map1);
            log.info("Modified jobParameters=" + jobParameters);
        }

        if (jobProperties != null && jobProperties.isRestart()) {
            if (jobExplorer == null) {
                throw new JobExecutionException("A JobExplorer must be provided for a restart or start next operation.");
            }
            JobExecution jobExecution = getLastFailedJobExecution(jobIdentifier);
            if (log.isDebugEnabled()) {
                log.info("Last failed JobExecution: " + jobExecution);
            }
            if (jobExecution == null && jobProperties.isFailRestart()) {
                throw new JobExecutionNotFailedException("No failed or stopped execution found for job="
                        + jobIdentifier);
            } else {
                log.info("No failed or stopped execution found for job=" + jobIdentifier
                        + ", batch properties flag for failRestart=" + jobProperties.isFailRestart()
                        + " so we don't fail restart.");
            }
            if (jobExecution != null) {
                restart = true;
                jobParameters = jobExecution.getJobParameters();
            }
        }

        if (jobProperties != null && jobProperties.isNext() && !restart) {
            if (jobExplorer == null) {
                throw new JobExecutionException("A JobExplorer must be provided for a restart or start next operation.");
            }
            JobParameters nextParameters = getNextJobParameters(job, false);
            Map<String, JobParameter> map = new HashMap<String, JobParameter>(nextParameters.getParameters());
            map.putAll(jobParameters.getParameters());
            jobParameters = new JobParameters(map);
            if (log.isDebugEnabled()) {
                log.info("JobParameter for job=[" + job + "] next=" + nextParameters + " used=" + jobParameters);
            }
        }

        JobExecution execution = this.jobLauncher.run(job, jobParameters);
        if (this.publisher != null) {
            this.publisher.publishEvent(new JobExecutionEvent(this, execution));
        }
    }

    /**
     * Get next job parameters for a job using {@link JobParametersIncrementer}.
     *
     * @param job the job that we need to find the next parameters for
     * @param fail suppress exception
     * @return the next job parameters if they can be located
     * @throws JobParametersNotFoundException if there is a problem
     */
    private JobParameters getNextJobParameters(Job job, boolean fail) throws JobParametersNotFoundException {
        String jobIdentifier = job.getName();
        JobParameters jobParameters;
        List<JobInstance> lastInstances = jobExplorer.getJobInstances(jobIdentifier, 0, 1);

        JobParametersIncrementer incrementer = job.getJobParametersIncrementer();
        if (incrementer == null) {
            throw new JobParametersNotFoundException("No job parameters incrementer found for job=" + jobIdentifier);
        }

        if (lastInstances.isEmpty()) {
            jobParameters = incrementer.getNext(new JobParameters());
            if (jobParameters == null) {
                throw new JobParametersNotFoundException("No bootstrap parameters found from incrementer for job="
                        + jobIdentifier);
            }
        } else {
            List<JobExecution> lastExecutions = jobExplorer.getJobExecutions(lastInstances.get(0));
            jobParameters = incrementer.getNext(lastExecutions.get(0).getJobParameters());
        }
        return jobParameters;
    }

    /**
     * Gets the last failed job execution. Doesn't returns failed job execution
     * if there is an successful execution.
     *
     * @param jobIdentifier the job identifier
     * @return the last failed job execution
     */
    private JobExecution getLastFailedJobExecution(String jobIdentifier) {
        List<JobExecution> jobExecutions = getJobExecutionsWithStatusGreaterThan(jobIdentifier);
        if (jobExecutions.isEmpty()) {
            return null;
        }

        if (!jobExecutions.get(0).getStatus().isUnsuccessful()) {
            return null;
        } else {
            return jobExecutions.get(0);
        }
    }

    /**
     * Gets the job executions.
     *
     * @param jobIdentifier a job execution id or job name
     * @return the job executions with status greater than
     */
    private List<JobExecution> getJobExecutionsWithStatusGreaterThan(String jobIdentifier) {
        int start = 0;
        int count = 100;
        List<JobExecution> executions = new ArrayList<JobExecution>();
        List<JobInstance> lastInstances = jobExplorer.getJobInstances(jobIdentifier, start, count);

        while (!lastInstances.isEmpty()) {

            for (JobInstance jobInstance : lastInstances) {
                List<JobExecution> jobExecutions = jobExplorer.getJobExecutions(jobInstance);
                if (jobExecutions == null || jobExecutions.isEmpty()) {
                    continue;
                }
                for (JobExecution jobExecution : jobExecutions) {
                    executions.add(jobExecution);
                }
            }

            start += count;
            lastInstances = jobExplorer.getJobInstances(jobIdentifier, start, count);
        }

        return executions;
    }
}
