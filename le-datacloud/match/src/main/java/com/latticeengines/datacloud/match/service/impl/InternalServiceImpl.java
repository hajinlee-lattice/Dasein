package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.datasource.DataSourceService;
import com.latticeengines.datacloud.match.exposed.service.InternalService;
import com.latticeengines.domain.exposed.datacloud.DataSourcePool;

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
