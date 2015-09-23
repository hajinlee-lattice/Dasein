package com.latticeengines.cassandra.exposed.dao;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.latticeengines.cassandra.exposed.dao.data.Column;
import com.latticeengines.cassandra.exposed.dao.data.Table;

public interface CassandraGenericDao {

    void createTable(Table table);

    void dropTable(String table);

    void createIndex(String table, String column, String index);

    void dropIndex(String table, String index);

    void addColumn(String table, Column column);

    void dropColumn(String table, String column);

    void save(String table, Map<String, Object> map);

    void save(String table, List<Map<String, Object>> list);

    void update(String table, String column, Object value, String whereColumn, Object whereColumnValue);

    void delete(String table, String column, Object value);

    void deleteAll(String table);

    Map<String, Object> findByKey(String table, String column, String value);

    List<Map<String, Object>> findAll(String table);

    List<Map<String, Object>> queryForListOfMap(String cql);

    List<Map<String, Object>> queryForListOfMapByTableColumn(String table, String column, Object value, int limit);

    long count(String table);

    ResultSet query(String cql);

    Map<String, Object> findByKey2(String table, String whereColumn, String value);

}
