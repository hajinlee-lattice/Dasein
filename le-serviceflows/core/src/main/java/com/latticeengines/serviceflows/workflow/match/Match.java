package com.latticeengines.serviceflows.workflow.match;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.datacloud.CommandParameter;
import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.CreateCommandRequest;
import com.latticeengines.domain.exposed.datacloud.MatchCommandStatus;
import com.latticeengines.domain.exposed.datacloud.MatchStatusResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("match")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class Match extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(Match.class);

    private static final EnumSet<MatchCommandStatus> TERMINAL_MATCH_STATUS = EnumSet.of(MatchCommandStatus.COMPLETE,
            MatchCommandStatus.ABORTED, MatchCommandStatus.FAILED);

    @Inject
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

            SleepUtils.sleep(10000L);

        } while (matchCommandStatus != null && !TERMINAL_MATCH_STATUS.contains(matchCommandStatus));
    }
}
