package com.latticeengines.propdata.api.entitymanager;

import java.util.Collection;

import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;

public interface PropDataEntityMgr {

    Command createCommand(String sourceTable, String contractExternalID, String destTables);

    Command getCommand(Long pid);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    void dropTable(String tableName);

    Collection<String> generatedResultTables(Long commandId);

    boolean resultTablesAreReady(Long commandId);
}
