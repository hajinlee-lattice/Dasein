package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.dao.CommandDao;
import com.latticeengines.datacloud.match.dao.CommandParameterDao;
import com.latticeengines.datacloud.match.datasource.MatchClientRoutingDataSource;
import com.latticeengines.datacloud.match.entitymgr.CommandEntityMgr;
import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.MatchCommandStatus;

@Component
public class CommandEntityMgrImpl implements CommandEntityMgr {

    @Autowired
    private CommandDao commandDao;

    @Autowired
    private CommandParameterDao commandParameterDao;

    @Autowired
    private MatchClientRoutingDataSource dataSource;

    private JdbcTemplate jdbcTemplate = new JdbcTemplate();

    public CommandEntityMgrImpl() {
        super();
    }

    @PostConstruct
    private void linkDataSourceToJdbcTemplate() {
        jdbcTemplate.setDataSource(dataSource);
    }

    @Override
    @Transactional(value = "propdata", propagation = Propagation.REQUIRED)
    synchronized public Commands createCommand(String sourceTable, String contractExternalID, String destTables,
                                               Map<String, String> parameters) {
        Commands command = commandDao.createCommandByStoredProcedure(sourceTable, contractExternalID, destTables);
        if (parameters != null && !parameters.isEmpty()) {
            for (Map.Entry<String, String> entry: parameters.entrySet()) {
                commandParameterDao.registerParameter(command.getProcessUID(), entry.getKey(), entry.getValue());
            }
        }
        return command;
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public Commands getCommand(Long pid) {
        return commandDao.findByKey(Commands.class, pid);
    }

    @Override
    @Transactional(value = "propdata", readOnly = true)
    public MatchCommandStatus getMatchCommandStatus(Long commandID) {
        return commandDao.getMatchCommandStatus(commandID);
    }


    @Override
    @Transactional(value = "propdata", readOnly = true)
    public Collection<String> generatedResultTables(Long commandId) {
        Commands command = getCommand(commandId);
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
        Commands command = getCommand(commandId);
        Set<String> targetTables = mangledResultTables(command);
        for (String table: targetTables) {
            if (!tableExists(table)) {
                return false;
            }
        }
        return true;
    }

    private Set<String> mangledResultTables(Commands command) {
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
