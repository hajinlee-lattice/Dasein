package com.latticeengines.db.dao.cassandra.impl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.keyspace.CreateIndexSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.DropIndexSpecification;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.latticeengines.db.dao.cassandra.CassandraGenericDao;

public class CassandraGenericDaoImpl implements CassandraGenericDao {

	private CassandraOperations template;

	@Autowired
	public CassandraGenericDaoImpl(CassandraOperations template) {
		this.template = template;
	}

	@Override
	public void createTable(String table) {
		CreateTableSpecification spec = CreateTableSpecification
				.createTable(table);
		template.execute(spec);
	}

	@Override
	public void dropTable(String table) {
		DropTableSpecification spec = DropTableSpecification.dropTable(table);
		template.execute(spec);

	}

	@Override
	public void createIndex(String table, String column, String index) {
		CreateIndexSpecification spec = CreateIndexSpecification
				.createIndex(table);
		spec.columnName(column).name(index);
		template.execute(spec);
	}

	@Override
	public void dropIndex(String table, String index) {
		DropIndexSpecification spec = DropIndexSpecification.dropIndex();
		spec.name(index);
		template.execute(spec);

	}

	@Override
	public void save(String table, Map<String, Object> map) {
		// TODO Auto-generated method stub

	}

	@Override
	public void save(String table, List<Map<String, Object>> list) {
		// TODO Auto-generated method stub

	}

	@Override
	public void update(String table, String column, Object value, String key,
			Object keyValue) {
		Update update = QueryBuilder.update(table);
		update.with(QueryBuilder.set(column, value));
		update.where(QueryBuilder.eq(key, keyValue));
		template.execute(update);
	}

	@Override
	public void delete(String table, String column, Object value) {
		Delete delete = QueryBuilder.delete().from(table);
		delete.where(QueryBuilder.eq(column, value));
		template.execute(delete);
	}

	@Override
	public void deleteAll(String table) {
		template.truncate(table);
	}

	@Override
	public Map<String, Object> findByKey(String table, String column,
			String value) {
		Select select = QueryBuilder.select().from(table);
		select.where(QueryBuilder.eq(column, value));
		return template.queryForMap(select);
	}

	@Override
	public List<Map<String, Object>> findAll(String table) {
		Select select = QueryBuilder.select().from(table);
		return template.queryForListOfMap(select);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMap(String cql) {
		return template.queryForListOfMap(cql);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMapByTableColumn(
			String table, String column, Object value) {

		Select select = QueryBuilder.select().from(table);
		select.where(QueryBuilder.eq(column, value));
		return template.queryForListOfMap(select);
	}

	@Override
	public long count(String table) {
		return template.count(table);
	}

}
