package com.latticeengines.propdata.eai.entitymanager;

public interface PropDataEntityMgr {

    void dropTable(String tableName);

    void executeQueryUpdate(String sql);

    void executeProcedure(String procedure);
}
