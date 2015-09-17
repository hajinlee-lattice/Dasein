package com.latticeengines.propdata.eai.entitymanager;

public interface PropDataEntityMgr {

//    void createCommands(Commands commands);
//
//    Commands getCommands(Long pid);
//
//    CommandIds getCommandIds(Long pid);

    void dropTable(String tableName);

    void executeQueryUpdate(String sql);

    void executeProcedure(String procedure);
}
