package com.latticeengines.serviceflows.workflow.match;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.CommandParameter;
import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.CreateCommandRequest;
import com.latticeengines.domain.exposed.datacloud.MatchCommandStatus;
import com.latticeengines.domain.exposed.datacloud.MatchStatusResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("match")
public class Match extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Log log = LogFactory.getLog(Match.class);

    private static final EnumSet<MatchCommandStatus> TERMINAL_MATCH_STATUS = EnumSet.of(MatchCommandStatus.COMPLETE,
            MatchCommandStatus.ABORTED, MatchCommandStatus.FAILED);

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Override
    public void execute() {
        log.info("Inside Match execute()");
        Table preMatchEventTable = getObjectFromContext(PREMATCH_EVENT_TABLE, Table.class);

        Long matchCommandId = match(preMatchEventTable);

        putLongValueInContext(MATCH_COMMAND_ID, matchCommandId);
    }

    private Long match(Table preMatchEventTable) {
        CreateCommandRequest matchCommand = new CreateCommandRequest();
        matchCommand.setSourceTable(preMatchEventTable.getName());
        matchCommand.setCommandType(configuration.getMatchCommandType());
        matchCommand.setContractExternalID(configuration.getCustomerSpace().toString());
        matchCommand.setDestTables(configuration.getDestTables());

        Map<String, String> commandParameters = new HashMap<>();
        commandParameters.put(CommandParameter.KEY_INTERPRETED_DOMAIN, CommandParameter.VALUE_YES);
        commandParameters.put(CommandParameter.JOIN_TYPE, configuration.getMatchJoinType().name());
        matchCommand.setParameters(commandParameters);

        Commands response = matchCommandProxy.createMatchCommand(matchCommand, configuration.getMatchClient());

        waitForMatchCommand(response);

        return response.getPid();
    }

    private void waitForMatchCommand(Commands commands) {
        log.info(String.format("Waiting for match command %d to complete", commands.getPid()));

        MatchCommandStatus matchCommandStatus = null;
        do {
            MatchStatusResponse status = matchCommandProxy.getMatchStatus(commands.getPid(),
                    configuration.getMatchClient());
            if (status == null) {
                throw new LedpException(LedpCode.LEDP_28009, new String[] { configuration.getMatchClient() });
            }

            matchCommandStatus = MatchCommandStatus.fromStatus(status.getStatus());
            log.info("Match Status = " + matchCommandStatus);

            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // Ignore InterruptedException
            }

        } while (matchCommandStatus != null && !TERMINAL_MATCH_STATUS.contains(matchCommandStatus));
    }
}
