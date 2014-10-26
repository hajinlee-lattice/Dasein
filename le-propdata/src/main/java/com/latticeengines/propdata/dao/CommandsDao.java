package com.latticeengines.propdata.dao;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.Commands;

public interface CommandsDao extends BaseDao<Commands> {

    void dropTable(String tableName);

    void executeQueryUpdate(String sql);

    void executeProcedure(String procedure);

}
