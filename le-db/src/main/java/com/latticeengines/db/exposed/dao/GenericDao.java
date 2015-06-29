package com.latticeengines.db.exposed.dao;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

public interface GenericDao {

    List<Map<String, Object>> queryForListOfMap(String sql, MapSqlParameterSource parameters);

    <T> T queryForObject(String sql, MapSqlParameterSource parameters, Class<T> requiredType);

    void update(String sql, MapSqlParameterSource parameters);
}
