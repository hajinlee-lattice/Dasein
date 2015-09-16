package com.latticeengines.propdata.api.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.Command;

public interface CommandDao extends BaseDao<Command> {

    void dropTable(String tableName);

    void executeQueryUpdate(String sql);

    void executeProcedure(String procedure);

}
