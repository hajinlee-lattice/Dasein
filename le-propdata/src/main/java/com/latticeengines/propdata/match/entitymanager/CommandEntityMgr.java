package com.latticeengines.propdata.match.entitymanager;

import java.util.Collection;
import java.util.Map;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;

public interface CommandEntityMgr {

    Commands createCommand(String sourceTable, String contractExternalID, String destTables, Map<String, String> parameters);

    Commands getCommand(Long pid);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    Collection<String> generatedResultTables(Long commandId);

    boolean resultTablesAreReady(Long commandId);
}
