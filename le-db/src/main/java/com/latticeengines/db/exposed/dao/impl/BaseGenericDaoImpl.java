package com.latticeengines.db.exposed.dao.impl;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.latticeengines.db.exposed.dao.GenericDao;

public class BaseGenericDaoImpl implements GenericDao {

    private NamedParameterJdbcTemplate namedJdbcTemplate;

    public BaseGenericDaoImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        this.namedJdbcTemplate = namedJdbcTemplate;
    }

    @Override
    public List<Map<String, Object>> queryForListOfMap(String sql, MapSqlParameterSource parameters) {
        return namedJdbcTemplate.queryForList(sql, parameters);
    }

    @Override
    public <T> T queryForObject(String sql, MapSqlParameterSource parameters,
            Class<T> requiredType) {
        return namedJdbcTemplate.queryForObject(sql, parameters, requiredType);
    }
}
