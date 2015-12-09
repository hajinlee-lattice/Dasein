package com.latticeengines.serviceflows.workflow.match;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("match")
public class Match extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Log log = LogFactory.getLog(Match.class);

    private static final EnumSet<MatchCommandStatus> TERMINAL_MATCH_STATUS = EnumSet.of(MatchCommandStatus.COMPLETE,
            MatchCommandStatus.FAILED);

    @Override
    public void execute() {
        log.info("Inside Match execute()");
        Table preMatchEventTable = JsonUtils.deserialize(executionContext.getString(PREMATCH_EVENT_TABLE), Table.class);

        Long matchCommandId = match(preMatchEventTable);

        executionContext.putLong(MATCH_COMMAND_ID, matchCommandId);
    }

    private Long match(Table preMatchEventTable) {
        CreateCommandRequest matchCommand = new CreateCommandRequest();
        matchCommand.setSourceTable(preMatchEventTable.getName());
        matchCommand.setCommandType(configuration.getMatchCommandType());
        matchCommand.setContractExternalID(configuration.getCustomerSpace().toString());
        matchCommand.setDestTables(configuration.getDestTables());

        String url = String.format("%s/propdata/matchcommands", configuration.getMicroServiceHostPort());
        Commands response = restTemplate.postForObject(url, matchCommand, Commands.class);

        waitForMatchCommand(response);

        return response.getPid();
    }

    @SuppressWarnings("unchecked")
    private void waitForMatchCommand(Commands commands) {
        Map<String, String> status = new HashMap<>();
        int maxTries = 1000;
        int i = 0;
        MatchCommandStatus matchCommandStatus = null;
        do {
            String url = String.format(configuration.getMicroServiceHostPort()
                    + "/propdata/matchcommands/%s?matchClient=%s", commands.getPid(), configuration.getMatchClient());
            status = restTemplate.getForObject(url, Map.class);
            if (status == null) {
                throw new LedpException(LedpCode.LEDP_28009, new String[] { url });
            }

            matchCommandStatus = MatchCommandStatus.fromStatus(status.get("Status"));
            log.info("Match Status = " + matchCommandStatus);

            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // Ignore InterruptedException
            }
            i++;

            if (i == maxTries) {
                break;
            }

        } while (matchCommandStatus != null && !TERMINAL_MATCH_STATUS.contains(matchCommandStatus));
    }

}
