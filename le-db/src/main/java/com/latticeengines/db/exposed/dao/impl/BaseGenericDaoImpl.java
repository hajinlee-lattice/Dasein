package com.latticeengines.db.exposed.dao.impl;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import com.latticeengines.db.exposed.dao.GenericDao;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class BaseGenericDaoImpl implements GenericDao {
    private static final Logger log = LoggerFactory.getLogger(BaseGenericDaoImpl.class);

    private NamedParameterJdbcTemplate namedJdbcTemplate;

    public BaseGenericDaoImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        this.namedJdbcTemplate = namedJdbcTemplate;
    }

    @Override
    public List<Map<String, Object>> queryForListOfMap(String sql, MapSqlParameterSource parameters) {
        return namedJdbcTemplate.queryForList(sql, parameters);
    }

    @Override
    public <T> T queryForObject(String sql, MapSqlParameterSource parameters, Class<T> requiredType) {
        return namedJdbcTemplate.queryForObject(sql, parameters, requiredType);
    }

    @Override
    public void update(String sql, MapSqlParameterSource parameters) {
        namedJdbcTemplate.update(sql, parameters);
    }

    @Override
    public List<Map<String, Object>> queryNativeSql(String sql, MapSqlParameterSource source) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            PreparedStatementCreator creator = getPreparedStatementCreator(sql, source);

            try (Connection conn = namedJdbcTemplate.getJdbcTemplate().getDataSource().getConnection()) {
                try (PreparedStatement ps = creator.createPreparedStatement(conn)) {
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Map<String, Object> row = new HashMap<>();
                            result.add(row);

                            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                                row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_12014, new String[] { e.getMessage() });
        }

        return result;
    }

    private PreparedStatementCreator getPreparedStatementCreator(String sql, MapSqlParameterSource source) {
        try {
            Class<?> clazz = namedJdbcTemplate.getClass();
            Method getPreparedStatementCreator = clazz.getDeclaredMethod("getPreparedStatementCreator", String.class,
                    SqlParameterSource.class);
            getPreparedStatementCreator.setAccessible(true);
            return (PreparedStatementCreator) getPreparedStatementCreator.invoke(namedJdbcTemplate, sql, source);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new LedpException(LedpCode.LEDP_12013);
        }
    }
}
