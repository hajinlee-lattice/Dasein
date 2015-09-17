package com.latticeengines.propdata.api.entitymanager.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.propdata.api.dao.CommandDao;
import com.latticeengines.propdata.api.datasource.MatchClientRoutingDataSource;
import com.latticeengines.propdata.api.entitymanager.PropDataEntityMgr;

@Component("propDataEntityMgr")
public class PropDataEntityMgrImpl implements PropDataEntityMgr {

    @Autowired
    private CommandDao commandDao;

    @Autowired
    private MatchClientRoutingDataSource dataSource;

    private JdbcTemplate jdbcTemplate = new JdbcTemplate();

    public PropDataEntityMgrImpl() {
        super();
    }

    @PostConstruct
    private void linkDataSourceToJdbcTemplate() {
        jdbcTemplate.setDataSource(dataSource);
    }

    @Override
    @Transactional(value = "propdata", propagation = Propagation.REQUIRED)
    synchronized public Command createCommand(String sourceTable, String contractExternalID, String destTables) {
        return commandDao.createCommandByStoredProcedure(sourceTable, contractExternalID, destTables);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public Command getCommand(Long pid) {
        return commandDao.findByKey(Command.class, pid);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public MatchCommandStatus getMatchCommandStatus(Long commandID) {
        return commandDao.getMatchCommandStatus(commandID);
    }

    @Override
    @Transactional(value = "propdata", readOnly = false)
    synchronized public void dropTable(String tableName) {
        commandDao.dropTable(tableName);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public Collection<String> generatedResultTables(Long commandId) {
        Command command = getCommand(commandId);
        Set<String> targetTables = mangledResultTables(command);
        for (String table: targetTables) {
            if (tableExists(table)) {
                targetTables.add(table);
            }
        }
        return targetTables;
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public boolean resultTablesAreReady(Long commandId) {
        Command command = getCommand(commandId);
        Set<String> targetTables = mangledResultTables(command);
        for (String table: targetTables) {
            if (!tableExists(table)) {
                return false;
            }
        }
        return true;
    }

    private Set<String> mangledResultTables(Command command) {
        String[] destTables = command.getDestTables().split("\\|");
        String commandName = command.getCommandName();
        Set<String> resultTables = new HashSet<>();
        for (String destTable: destTables) {
            String mangledTableName = String.format("%s_%s_%s",
                    commandName, String.valueOf(command.getPid()), destTable);
            resultTables.add(mangledTableName);
        }
        return resultTables;
    }

    private boolean tableExists(String tableName) {
        String sql = String.format("IF OBJECT_ID (N'dbo.%s', N'U') IS NOT NULL SELECT 1 ELSE SELECT 0", tableName);
        return jdbcTemplate.queryForObject(sql, Boolean.class);
    }

}
