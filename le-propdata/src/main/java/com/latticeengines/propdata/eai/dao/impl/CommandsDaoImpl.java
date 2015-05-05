package com.latticeengines.propdata.eai.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.propdata.eai.dao.CommandsDao;

public class CommandsDaoImpl extends BaseDaoImpl<Commands> implements CommandsDao {

    public CommandsDaoImpl() {
        super();
    }

    @Override
    protected Class<Commands> getEntityClass() {
        return Commands.class;
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