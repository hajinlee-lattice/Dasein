package com.latticeengines.propdata.api.dao.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.internal.SessionImpl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.propdata.api.dao.CommandDao;

public class CommandDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Commands> implements CommandDao {

    public CommandDaoImpl() { super(); }

    @Override
    protected Class<Commands> getEntityClass() {
        return Commands.class;
    }

    @Override
    public Commands createCommandByStoredProcedure(String sourceTable, String contractExternalID, String destTables) {
        try {
            Connection conn = ((SessionImpl) sessionFactory.getCurrentSession()).connection();
            CallableStatement cstmt = conn.prepareCall("{call dbo.MatcherClient_CreateCommand(?, ?, ?, ?)}");
            cstmt.setString("inputSourceTableName", sourceTable);
            cstmt.setString("contractExternalID", contractExternalID);
            cstmt.setString("destTables", destTables);
            cstmt.registerOutParameter("commandID", java.sql.Types.INTEGER);
            cstmt.execute();
            Long commandId = cstmt.getLong("commandID");
            cstmt.close();
            return findByKey(getEntityClass(), commandId);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create a command by stored procedure.", e);
        }
    }

    @Override
    public MatchCommandStatus getMatchCommandStatus(Long commandID) {
        try {
            Connection conn = ((SessionImpl) sessionFactory.getCurrentSession()).connection();
            CallableStatement cstmt = conn.prepareCall("{call dbo.MatcherClient_GetCommandStatus(?, ?)}");
            cstmt.setLong("commandID", commandID);
            cstmt.registerOutParameter("status", java.sql.Types.NVARCHAR);
            cstmt.execute();
            String status = cstmt.getString("status");
            cstmt.close();
            return MatchCommandStatus.fromStatus(status);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get command status by stored procedure.", e);
        }
    }
}