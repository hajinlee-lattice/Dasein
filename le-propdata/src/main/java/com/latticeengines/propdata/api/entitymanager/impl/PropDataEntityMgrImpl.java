package com.latticeengines.propdata.api.entitymanager.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.CommandId;
import com.latticeengines.propdata.api.dao.CommandIdDao;
import com.latticeengines.propdata.api.dao.CommandDao;
import com.latticeengines.propdata.api.entitymanager.PropDataEntityMgr;

@Component("propDataEntityMgr")
public class PropDataEntityMgrImpl implements PropDataEntityMgr {

    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private CommandIdDao commandIdsDao;

    @Autowired
    private CommandDao commandDao;

    public PropDataEntityMgrImpl() {
        super();
    }

    @Override
    @Transactional(value = "propdata", propagation = Propagation.REQUIRED)
    public void createCommands(Command commands) {
        CommandId commandIds = commands.getCommandIds();
        if (commandIds == null) {
            log.error("There's no CommandId for :" + commands);
            throw new IllegalStateException("There's no CommandId specified.");
        }

        commandIdsDao.create(commandIds);
        commands.setPid(commandIds.getPid());
        commandDao.create(commands);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public Command getCommands(Long pid) {
        return commandDao.findByKey(Command.class, pid);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public CommandId getCommandIds(Long pid) {
        return commandIdsDao.findByKey(CommandId.class, pid);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public void dropTable(String tableName) {
        commandDao.dropTable(tableName);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public void executeQueryUpdate(String sql) {
        commandDao.executeQueryUpdate(sql);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public void executeProcedure(String procedure) {
        commandDao.executeProcedure(procedure);
    }

}
