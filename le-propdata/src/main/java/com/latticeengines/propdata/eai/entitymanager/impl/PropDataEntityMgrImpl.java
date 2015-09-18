package com.latticeengines.propdata.eai.entitymanager.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.propdata.api.datasource.MatchClientRoutingDataSource;
import com.latticeengines.propdata.eai.entitymanager.PropDataEntityMgr;

@Component
public class PropDataEntityMgrImpl implements PropDataEntityMgr {

    @Resource(name="sessionFactoryPropDataMatch")
    private SessionFactory sessionFactory;

    @Autowired
    private MatchClientRoutingDataSource dataSource;

    private JdbcTemplate jdbcTemplate = new JdbcTemplate();

    public PropDataEntityMgrImpl() { super(); }

    @PostConstruct
    private void linkDataSourceToJdbcTemplate() {
        jdbcTemplate.setDataSource(dataSource);
    }

    @Override
    @Transactional(value = "propdataMatch", readOnly = true)
    public void dropTable(String tableName) {
        if (!tableExists(tableName)) {
            Session session = sessionFactory.getCurrentSession();
            Query query = session.createSQLQuery("DROP TABLE " + tableName);
            query.executeUpdate();
        }
    }

    @Override
    @Transactional(value = "propdataMatch")
    public void executeQueryUpdate(String sql) {
        Session session = sessionFactory.getCurrentSession();
        Query query = session.createSQLQuery(sql);
        query.executeUpdate();
    }

    @Override
    @Transactional(value = "propdataMatch")
    public void executeProcedure(String procedure) {
        try {
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(procedure);
            ps.execute();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private boolean tableExists(String tableName) {
        String sql = String.format("IF OBJECT_ID (N'%s', N'U') IS NOT NULL SELECT 1 ELSE SELECT 0", tableName);
        return jdbcTemplate.queryForObject(sql, Boolean.class);
    }

}
