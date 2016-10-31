package com.latticeengines.playmaker.entitymgr;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public interface JdbcTemplateFactory {
    NamedParameterJdbcTemplate getTemplate(String tenantName);
}
