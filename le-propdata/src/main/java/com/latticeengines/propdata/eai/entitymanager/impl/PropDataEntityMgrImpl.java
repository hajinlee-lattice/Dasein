package com.latticeengines.propdata.eai.entitymanager.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.propdata.api.dao.CommandDao;
import com.latticeengines.propdata.eai.entitymanager.PropDataEntityMgr;

@Component("propDataEntityMgr")
public class PropDataEntityMgrImpl implements PropDataEntityMgr {

    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private CommandDao commandsDao;

    public PropDataEntityMgrImpl() {
        super();
    }

    @Override
    @Transactional(value = "propdataMatch", readOnly = true)
    public void dropTable(String tableName) {
        commandsDao.dropTable(tableName);
    }

    @Override
    @Transactional(value = "propdataMatch", readOnly = true)
    public void executeQueryUpdate(String sql) {
        commandsDao.executeQueryUpdate(sql);
    }

    @Override
    @Transactional(value = "propdataMatch", readOnly = true)
    public void executeProcedure(String procedure) {
        commandsDao.executeProcedure(procedure);
    }

}
