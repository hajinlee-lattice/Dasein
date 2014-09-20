package com.latticeengines.db.dao.cassandra.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.stereotype.Component;

@Component("propDataCassandraGenericDao")
public class PropDataCassandraGenericDaoImpl extends CassandraGenericDaoImpl {

	@Autowired
	public PropDataCassandraGenericDaoImpl(
			@Qualifier("propDataCassandraTemplate") CassandraOperations template) {
		super(template);
	}
}
