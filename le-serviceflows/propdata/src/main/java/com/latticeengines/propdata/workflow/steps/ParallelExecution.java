package com.latticeengines.propdata.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.proxy.exposed.propdata.InternalProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("parallelExecution")
@Scope("prototype")
public class ParallelExecution extends BaseWorkflowStep<ParallelExecutionConfiguration> {

    private static Log log = LogFactory.getLog(ParallelExecution.class);
    private static int MAX_ERRORS = 100;

    @Autowired
    private InternalProxy internalProxy;

    private YarnClient yarnClient;
    private List<ApplicationId> applicationIds = new ArrayList<>();

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
        log.info("Inside ParallelExecution execute()");
        List<PropDataJobConfiguration> jobConfigurations = new ArrayList<>();

        Object listObj = executionContext.get(BulkMatchContextKey.YARN_JOB_CONFIGS);
        if(listObj instanceof List){
            @SuppressWarnings("rawtypes")
            List list = (List) executionContext.get(BulkMatchContextKey.YARN_JOB_CONFIGS);
            for(Object configObj : list){
                if(configObj instanceof PropDataJobConfiguration){
                    jobConfigurations.add((PropDataJobConfiguration)configObj);
                }
            }
        }

        List<ApplicationId> applicationIds = new ArrayList<>();
        for (PropDataJobConfiguration jobConfiguration : jobConfigurations) {
            ApplicationId appId = ConverterUtils.toApplicationId(internalProxy.submitYarnJob(jobConfiguration)
                    .getApplicationIds().get(0));
            log.info("Submit a match block to application id " + appId);
            applicationIds.add(appId);
        }

        initializeYarnClient();
        int numErrors = 0;
        try {
            for (ApplicationId appId : applicationIds) {
                try {
                    ApplicationReport report = yarnClient.getApplicationReport(appId);
                    YarnApplicationState state = report.getYarnApplicationState();
                    Float progress = report.getProgress();
                    String logMessage = String.format("Matcher [%s] is at state [%s]", appId, state);
                    if (YarnApplicationState.RUNNING.equals(state)) {
                        logMessage += String.format(": %.2f ", progress) + "%";
                    }
                    log.info(logMessage);
                } catch (Exception e) {
                    numErrors++;
                    log.error("Failed to read status of application " + appId, e);
                    if (numErrors > MAX_ERRORS) {
                        killChildrenApplications();
                        throw new RuntimeException("Exceeded maximum number of errors " + MAX_ERRORS);
                    }
                }
                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } finally {
            yarnClient.stop();
        }
    }

    private void initializeYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
    }

    private void killChildrenApplications() {
        for (ApplicationId appId: applicationIds) {
            try {
                yarnClient.killApplication(appId);
            } catch (Exception e) {
                log.error("Error when killing the application " + appId, e);
            }
        }
    }

}
