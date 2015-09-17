package com.latticeengines.propdata.api.dao.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.propdata.api.dao.CommandDao;

public class CommandDaoImpl extends BaseDaoImpl<Command> implements CommandDao {

    public CommandDaoImpl() { super(); }

    @Override
    protected Class<Command> getEntityClass() {
        return Command.class;
    }

    @Override
    public Command createCommandByStoredProcedure(String sourceTable, String contractExternalID, String destTables) {
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

    @Override
    public void dropTable(String tableName) {
        Session session = sessionFactory.getCurrentSession();
        Query query = session.createSQLQuery("DROP TABLE " + tableName);
        query.executeUpdate();
    }

    @Override
    public void executeQueryUpdate(String sql) {
        Session session = sessionFactory.getCurrentSession();
        Query query = session.createSQLQuery(sql);
        query.executeUpdate();
    }

    @Override
    public void executeProcedure(String procedure) {
        try {
            Connection connection = ((SessionImpl) sessionFactory.getCurrentSession()).connection();
            PreparedStatement ps = connection.prepareStatement(procedure);
            ps.execute();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}