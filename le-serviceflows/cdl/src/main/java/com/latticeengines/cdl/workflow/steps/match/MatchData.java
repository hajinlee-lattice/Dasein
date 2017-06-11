package com.latticeengines.cdl.workflow.steps.match;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.match.MatchConfiguration;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("matchData")
public class MatchData extends BaseWorkflowStep<MatchConfiguration> {

    protected static final long WORKFLOW_WAIT_TIME_IN_MILLIS = 1000L * 60 * 90;
    
    @Autowired
    private MatchProxy matchProxy;
    
    @Override
    public void execute() {
        MatchCommand command = matchProxy.matchBulk(configuration.getMatchInput(), CamilleEnvironment.getPodId());
        waitForMatchCommand(command);
    }

    private void waitForMatchCommand(MatchCommand matchCommand) {
        String rootUid = matchCommand.getRootOperationUid();
        String appId = matchCommand.getApplicationId();
        if (StringUtils.isEmpty(appId)) {
            appId = "null";
        }
        log.info(String.format("Waiting for match command %s [ApplicationId=%s] to complete", rootUid, appId));

        MatchStatus status = null;
        do {
            matchCommand = matchProxy.bulkMatchStatus(rootUid);
            status = matchCommand.getMatchStatus();
            if (status == null) {
                throw new LedpException(LedpCode.LEDP_28024, new String[] { rootUid });
            }
            appId = matchCommand.getApplicationId();
            if (StringUtils.isEmpty(appId)) {
                appId = "null";
            }
            String logMsg = "[ApplicationId=" + appId + "] Match Status = " + status;
            if (MatchStatus.MATCHING.equals(status)) {
                Float progress = matchCommand.getProgress();
                logMsg += String.format(": %.2f %%", progress * 100);
            }
            log.info(logMsg);

            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // Ignore InterruptedException
            }

        } while (!status.isTerminal());

        if (!MatchStatus.FINISHED.equals(status)) {
            throw new IllegalStateException("The terminal status of match is " + status + " instead of "
                    + MatchStatus.FINISHED);
        }

    }

}
