package com.latticeengines.cassandra.exposed.dao.impl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropColumnCqlGenerator;
import org.springframework.cassandra.core.keyspace.AlterTableSpecification;
import org.springframework.cassandra.core.keyspace.CreateIndexSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.cassandra.core.keyspace.DropColumnSpecification;
import org.springframework.cassandra.core.keyspace.DropIndexSpecification;
import org.springframework.cassandra.core.keyspace.DropTableSpecification;
import org.springframework.data.cassandra.core.CassandraOperations;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.latticeengines.cassandra.exposed.dao.CassandraGenericDao;
import com.latticeengines.cassandra.exposed.dao.data.Column;
import com.latticeengines.cassandra.exposed.dao.data.ColumnTypeMapper;
import com.latticeengines.cassandra.exposed.dao.data.Table;

public class CassandraGenericDaoImpl implements CassandraGenericDao {

	protected CassandraOperations template;

	@Autowired
	public CassandraGenericDaoImpl(CassandraOperations template) {
		this.template = template;
	}

	@Override
	public void createTable(Table table) {

		CreateTableSpecification spec = CreateTableSpecification
				.createTable(table.getTableName());
		spec.ifNotExists(true);
		for (Column column : table.getPartitionColumns()) {
			spec.partitionKeyColumn(column.getColumnName(),
					ColumnTypeMapper.toCQLType(column.getColumnType()));
		}
		for (Column column : table.getClusterColumns()) {
			spec.clusteredKeyColumn(column.getColumnName(),
					ColumnTypeMapper.toCQLType(column.getColumnType()));
		}
		for (Column column : table.getNormalColumns()) {
			spec.column(column.getColumnName(),
					ColumnTypeMapper.toCQLType(column.getColumnType()));
		}
		template.execute(spec);
	}

	@Override
	public void dropTable(String table) {
		DropTableSpecification spec = DropTableSpecification.dropTable(table);
		template.execute(spec);
	}

	@Override
	public void addColumn(String table, Column column) {
		/*
		 * AlterTableSpecification spec = AlterTableSpecification
		 * .alterTable(table); spec.add(column.getColumnName(),
		 * ColumnTypeMapper.toCQLType(column.getColumnType()));
		 */

		String sql = "ALTER TABLE " + table + " ADD " + column.getColumnName()
				+ " " + ColumnTypeMapper.toCQLType(column.getColumnType());
		template.execute(sql);
	}

	@Override
	public void dropColumn(String table, String column) {

		AlterTableSpecification tableSpec = AlterTableSpecification
				.alterTable(table);
		AlterTableCqlGenerator tableGenerator = new AlterTableCqlGenerator(
				tableSpec);

		DropColumnSpecification columnSpec = DropColumnSpecification
				.dropColumn(column);
		DropColumnCqlGenerator columnGenerator = new DropColumnCqlGenerator(
				columnSpec);
		StringBuilder tableSql = tableGenerator.toCql(new StringBuilder());
		StringBuilder sql = columnGenerator.toCql(tableSql);
		String sqlString = sql.toString().replaceAll(";", "");
		template.execute(sqlString);
	}

	@Override
	public void createIndex(String table, String column, String index) {
		CreateIndexSpecification spec = CreateIndexSpecification
				.createIndex(index);
		spec.tableName(table).columnName(column);
		spec.ifNotExists();
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
		Insert insert = QueryBuilder.insertInto(table);
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			insert.value(entry.getKey(), entry.getValue());
		}
		template.execute(insert);
	}

	@Override
	public void save(String table, List<Map<String, Object>> list) {
		for (Map<String, Object> map : list) {
			save(table, map);
		}
	}

	@Override
	public void update(String table, String column, Object value,
			String whereColumn, Object whereColumnValue) {
		Update update = QueryBuilder.update(table);
		update.with(QueryBuilder.set(column, value));
		update.where(QueryBuilder.eq(whereColumn, whereColumnValue));
		template.execute(update);
	}

	@Override
	public void delete(String table, String whereColumn, Object value) {
		Delete delete = QueryBuilder.delete().from(table);
		delete.where(QueryBuilder.eq(whereColumn, value));
		template.execute(delete);
	}

	@Override
	public void deleteAll(String table) {
		template.truncate(table);
	}

	@Override
	public Map<String, Object> findByKey(String table, String whereColumn,
			String value) {
		Select select = QueryBuilder.select().from(table);
		select.where(QueryBuilder.eq(whereColumn, value));
		select.allowFiltering();
		return template.queryForMap(select);
	}

	@Override
	public Map<String, Object> findByKey2(String table, String whereColumn,
			String value) {
		String cql = "SELECT * FROM " + table + " WHERE " + whereColumn
				+ " = \'" + value + "\'";
		ResultSet resultSet = query(cql);
		Row row = resultSet.one();
		return ColumnTypeMapper.convertRowToMap(row);
	}

	@Override
	public List<Map<String, Object>> findAll(String table) {
		Select select = QueryBuilder.select().from(table);
		return template.queryForListOfMap(select);
	}

	@Override
	public ResultSet query(String cql) {
		return template.query(cql);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMap(String cql) {
		return template.queryForListOfMap(cql);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMapByTableColumn(
			String table, String column, Object value, int limit) {

		Select select = QueryBuilder.select().from(table);
		select.where(QueryBuilder.eq(column, value));
		select.limit(limit);
		return template.queryForListOfMap(select);
	}

	@Override
	public long count(String table) {
		return template.count(table);
	}

}
