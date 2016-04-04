package com.latticeengines.propdata.match.service;

import java.util.Collection;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;

public interface MatchCommandsService {

    Commands createMatchCommand(CreateCommandRequest request);

    Commands findMatchCommandById(Long commandId);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    Collection<String> generatedResultTables(Long commandId);

    boolean resultTablesAreReady(Long commandId);

    MatchClientDocument getBestMatchClient(int numRows);

    MatchClient getMatchClientByName(String clientName);

}
