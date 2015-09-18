package com.latticeengines.propdata.api.entitymanager;

import java.util.Collection;

import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;

public interface CommandEntityMgr {

    Command createCommand(String sourceTable, String contractExternalID, String destTables);

    Command getCommand(Long pid);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    Collection<String> generatedResultTables(Long commandId);

    boolean resultTablesAreReady(Long commandId);
}
