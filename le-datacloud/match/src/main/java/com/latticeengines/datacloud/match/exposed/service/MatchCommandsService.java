package com.latticeengines.datacloud.match.exposed.service;

import java.util.Collection;

import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.CreateCommandRequest;
import com.latticeengines.domain.exposed.datacloud.MatchClient;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandStatus;

public interface MatchCommandsService {

    Commands createMatchCommand(CreateCommandRequest request);

    Commands findMatchCommandById(Long commandId);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    Collection<String> generatedResultTables(Long commandId);

    boolean resultTablesAreReady(Long commandId);

    MatchClientDocument getBestMatchClient(int numRows);

    MatchClient getMatchClientByName(String clientName);

}
