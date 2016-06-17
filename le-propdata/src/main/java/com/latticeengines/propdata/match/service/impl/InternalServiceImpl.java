package com.latticeengines.propdata.match.service.impl;

import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.match.service.InternalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component("internalService")
public class InternalServiceImpl implements InternalService {

    @Autowired
    private DataSourceService dataSourceService;

    @Override
    public Date currentCacheTableCreatedTime() {
        JdbcTemplate jdbcTemplate = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);
        Date createdTime = jdbcTemplate.queryForObject("SELECT TOP 1 [Timestamp] "
                + "FROM [LDC_SourceDB].[dbo].[DerivedColumnsCache_Status] ORDER BY [Timestamp] DESC", Date.class);
        return createdTime;
    }
}
