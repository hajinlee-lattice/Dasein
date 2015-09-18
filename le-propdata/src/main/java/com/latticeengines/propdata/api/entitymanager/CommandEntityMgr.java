package com.latticeengines.propdata.api.entitymanager;

import java.util.Collection;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;

public interface CommandEntityMgr {

    Commands createCommand(String sourceTable, String contractExternalID, String destTables);

    Commands getCommand(Long pid);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    Collection<String> generatedResultTables(Long commandId);

    boolean resultTablesAreReady(Long commandId);
}
