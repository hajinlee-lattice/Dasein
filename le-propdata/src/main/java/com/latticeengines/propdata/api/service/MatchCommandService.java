package com.latticeengines.propdata.api.service;

import java.util.Collection;

import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;

public interface MatchCommandService {

    Command createMatchCommand(CreateCommandRequest request);

    Command findMatchCommandById(Long commandId);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    Collection<String> generatedResultTables(Long commandId);

    boolean resultTablesAreReady(Long commandId);

}
