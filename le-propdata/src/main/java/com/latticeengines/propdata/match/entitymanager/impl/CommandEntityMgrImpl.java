package com.latticeengines.propdata.match.entitymanager.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.propdata.match.dao.CommandDao;
import com.latticeengines.propdata.match.datasource.MatchClientRoutingDataSource;
import com.latticeengines.propdata.match.entitymanager.CommandEntityMgr;

@Component
public class CommandEntityMgrImpl implements CommandEntityMgr {

    @Autowired
    private CommandDao commandDao;

    @Resource(name="sessionFactoryPropDataMatch")
    private SessionFactory sessionFactory;

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
    synchronized public Commands createCommand(String sourceTable, String contractExternalID, String destTables) {
        return commandDao.createCommandByStoredProcedure(sourceTable, contractExternalID, destTables);
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
