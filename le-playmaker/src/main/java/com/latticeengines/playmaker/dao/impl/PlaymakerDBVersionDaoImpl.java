package com.latticeengines.playmaker.dao.impl;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.playmaker.dao.PlaymakerDBVersionDao;

public class PlaymakerDBVersionDaoImpl extends BaseGenericDaoImpl implements PlaymakerDBVersionDao {

    public PlaymakerDBVersionDaoImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public String getDBVersion() {
        String sql = "SELECT TOP 1 Build_App_Version FROM [DBVersionHistory] WITH (NOLOCK) ORDER BY PatchNumber DESC";

        MapSqlParameterSource source = new MapSqlParameterSource();
        return queryForObject(sql, source, String.class);

    }
}