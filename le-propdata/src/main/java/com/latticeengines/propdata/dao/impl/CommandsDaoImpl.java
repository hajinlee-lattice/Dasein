package com.latticeengines.propdata.dao.impl;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.dataplatform.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.propdata.dao.CommandsDao;

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


}