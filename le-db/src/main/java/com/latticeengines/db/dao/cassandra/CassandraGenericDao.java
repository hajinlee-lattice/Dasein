package com.latticeengines.db.dao.cassandra;

import java.util.List;
import java.util.Map;

public interface CassandraGenericDao {

	void createTable(String table);

	void dropTable(String table);

	void createIndex(String table, String column, String index);

	void dropIndex(String table, String index);

	void save(String table, Map<String, Object> map);

	void save(String table, List<Map<String, Object>> list);

	void update(String table, String column, Object value, String key,
			Object keyValue);

	void delete(String table, String column, Object value);

	void deleteAll(String table);

	Map<String, Object> findByKey(String table, String column, String value);

	List<Map<String, Object>> findAll(String table);

	List<Map<String, Object>> queryForListOfMap(String cql);

	List<Map<String, Object>> queryForListOfMapByTableColumn(String table,
			String column, Object value);

	long count(String table);

}
