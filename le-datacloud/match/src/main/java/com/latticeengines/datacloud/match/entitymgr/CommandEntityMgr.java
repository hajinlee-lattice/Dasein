package com.latticeengines.datacloud.match.entitymgr;

import java.util.Collection;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.MatchCommandStatus;

public interface CommandEntityMgr {

    Commands createCommand(String sourceTable, String contractExternalID, String destTables, Map<String, String> parameters);

    Commands getCommand(Long pid);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    Collection<String> generatedResultTables(Long commandId);

    boolean resultTablesAreReady(Long commandId);
}
