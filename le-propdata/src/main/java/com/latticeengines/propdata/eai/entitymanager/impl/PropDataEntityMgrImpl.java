package com.latticeengines.propdata.eai.entitymanager.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.CommandIds;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.propdata.eai.dao.CommandIdsDao;
import com.latticeengines.propdata.eai.dao.CommandsDao;
import com.latticeengines.propdata.eai.entitymanager.PropDataEntityMgr;

@Component("propDataEntityMgr")
public class PropDataEntityMgrImpl implements PropDataEntityMgr {

    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private CommandIdsDao commandIdsDao;

    @Autowired
    private CommandsDao commandsDao;

    public PropDataEntityMgrImpl() {
        super();
    }

    @Override
    @Transactional(value = "propdata", propagation = Propagation.REQUIRED)
    public void createCommands(Commands commands) {
        CommandIds commandIds = commands.getCommandIds();
        if (commandIds == null) {
            log.error("There's no CommandIds for :" + commands);
            throw new IllegalStateException("There's no CommandIds specified.");
        }

        commandIdsDao.create(commandIds);
        commands.setPid(commandIds.getPid());
        commandsDao.create(commands);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public Commands getCommands(Long pid) {
        return commandsDao.findByKey(Commands.class, pid);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public CommandIds getCommandIds(Long pid) {
        return commandIdsDao.findByKey(CommandIds.class, pid);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public void dropTable(String tableName) {
        commandsDao.dropTable(tableName);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public void executeQueryUpdate(String sql) {
        commandsDao.executeQueryUpdate(sql);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public void executeProcedure(String procedure) {
        commandsDao.executeProcedure(procedure);
    }

}
