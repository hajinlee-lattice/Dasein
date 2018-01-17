# update status which are not enum of JobStatus (PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, SKIPPED)
# UPDATE WORKFLOW_JOB
# SET STATUS = CASE
#              WHEN 'SUCCEEDED' THEN 'COMPLETED'
#              WHEN 'STOPPED' THEN 'CANCELLED'
#              WHEN 'ABANDONED' OR 'KILLED' OR 'UNKNOWN' THEN 'FAILED'
#              WHEN 'STARTING' OR 'UNDEFINED' THEN 'PENDING'
#              WHEN 'STARTED' OR 'STOPPING' THEN 'RUNNING'
#              END;

# update null status
UPDATE WORKFLOW_JOB job
  JOIN WORKFLOW_JOB_EXECUTION jobExecution ON job.WORKFLOW_ID = jobExecution.JOB_EXECUTION_ID
SET job.STATUS = CASE
                 WHEN jobExecution.STATUS = 'ABANDONED' OR jobExecution.STATUS = 'FAILED' OR jobExecution.STATUS = 'UNKNOWN' THEN 'FAILED'
                 WHEN jobExecution.STATUS = 'STOPPED' THEN 'CANCELLED'
                 WHEN jobExecution.STATUS = 'STARTED' OR jobExecution.STATUS = 'STOPPING' OR jobExecution.STATUS = 'STARTING' THEN 'RUNNING'
                 WHEN jobExecution.STATUS = 'COMPLETED' THEN 'COMPLETED'
                 END
WHERE job.STATUS IS NULL;

# update null type
UPDATE WORKFLOW_JOB job
  JOIN WORKFLOW_JOB_EXECUTION execution ON job.WORKFLOW_ID = execution.JOB_EXECUTION_ID
  JOIN WORKFLOW_JOB_INSTANCE instance ON execution.JOB_INSTANCE_ID = instance.JOB_INSTANCE_ID
SET job.TYPE = instance.JOB_NAME
WHERE job.TYPE IS NULL;
