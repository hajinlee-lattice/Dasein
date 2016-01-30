package com.latticeengines.propdata.match.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;

public interface CommandDao extends BaseDao<Commands> {

    Commands createCommandByStoredProcedure(String sourceTable, String contractExternalID, String destTables);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

}
