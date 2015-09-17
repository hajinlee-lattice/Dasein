package com.latticeengines.propdata.api.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;

public interface CommandDao extends BaseDao<Command> {

    Command createCommandByStoredProcedure(String sourceTable, String contractExternalID, String destTables);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

    void dropTable(String tableName);

    void executeQueryUpdate(String sql);

    void executeProcedure(String procedure);

}
