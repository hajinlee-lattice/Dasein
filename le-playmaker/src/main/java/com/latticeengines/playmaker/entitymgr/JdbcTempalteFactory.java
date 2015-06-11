package com.latticeengines.playmaker.entitymgr;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public interface JdbcTempalteFactory {
    NamedParameterJdbcTemplate getTemplate(String tenantName);
}
