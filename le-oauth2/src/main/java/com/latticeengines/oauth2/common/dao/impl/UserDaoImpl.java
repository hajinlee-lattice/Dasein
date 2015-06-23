package com.latticeengines.oauth2.common.dao.impl;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.oauth2.common.dao.UserDao;

//@Component
public class UserDaoImpl extends BaseGenericDaoImpl implements UserDao {

    public UserDaoImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public List<Map<String, Object>> getUserByName(String userName) {
        String sql = "SELECT username, password, enabled  WHERE username = :username";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("username", userName);
        return queryForListOfMap(sql, source);
    }

}
