package com.latticeengines.dcp.workflow.listeners;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.dataReport.DataReportRollupStatus;
import com.latticeengines.proxy.exposed.dcp.DataReportProxy;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("dataReportListener")
public class DataReportListener extends LEJobListener {

    public static final Logger log = LoggerFactory.getLogger(DataReportListener.class);

    @Inject
    DataReportProxy dataReportProxy;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        log.info("Before job execution, set status to RUNNING");
        String customerSpace = jobExecution.getJobParameters().getString("CustomerSpace");
        dataReportProxy.updateRollupStatus(customerSpace,
                new DataReportRollupStatus(DataReportRecord.RollupStatus.RUNNING));
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        log.info("Checking for errors in roll up data report");
        ExitStatus exitStatus = jobExecution.getExitStatus();
        if (ExitStatus.FAILED.getExitCode().equals(exitStatus.getExitCode())) {
            failTheWorkflowAndUpdateDataReport(jobExecution);
        } else {
            successUpdateDataReport(jobExecution);
        }
        log.info("Finish roll up data report");
    }

    private void failTheWorkflowAndUpdateDataReport(JobExecution jobExecution) {
        String customerSpace = jobExecution.getJobParameters().getString("CustomerSpace");
        ExitStatus exitStatus = jobExecution.getExitStatus();
        String msg = String.format("%s -- %s", exitStatus.getExitCode(),
                exitStatus.getExitDescription());
        dataReportProxy.updateRollupStatus(customerSpace,
                new DataReportRollupStatus(DataReportRecord.RollupStatus.FAILED_NO_RETRY, exitStatus.getExitCode(), exitStatus.getExitDescription()));
        log.warn("Failed data report job. Exit status {}", msg);
    }

    private void successUpdateDataReport(JobExecution jobExecution) {
        String customerSpace = jobExecution.getJobParameters().getString("CustomerSpace");
        dataReportProxy.updateRollupStatus(customerSpace,
                new DataReportRollupStatus(DataReportRecord.RollupStatus.READY));
        log.info("Successful finish of Data Report job.");
    }
}
