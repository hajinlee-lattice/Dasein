package com.latticeengines.propdata.eai.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.Commands;

public interface CommandsDao extends BaseDao<Commands> {

    void dropTable(String tableName);

    void executeQueryUpdate(String sql);

    void executeProcedure(String procedure);

}
