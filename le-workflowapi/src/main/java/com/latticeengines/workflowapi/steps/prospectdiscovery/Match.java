package com.latticeengines.workflowapi.steps.prospectdiscovery;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;

@Component("match")
public class Match extends BaseFitModelStep<BaseFitModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(Match.class);

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
        matchCommand.setCommandType(MatchCommandType.MATCH_WITH_UNIVERSE);
        matchCommand.setContractExternalID(configuration.getCustomerSpace());
        matchCommand.setDestTables("DerivedColumns");

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
        do {
            String url = String.format(configuration.getMicroServiceHostPort() + "/propdata/matchcommands/%s?matchClient=PD130",
                    commands.getPid());
            status = restTemplate.getForObject(url, Map.class);
            System.out.println("Status = " + status.get("Status"));
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                // Ignore InterruptedException
            }
            i++;

            if (i == maxTries) {
                break;
            }
        } while (status != null && !status.get("Status").equals("COMPLETE"));
    }

}
