package com.latticeengines.playmaker.entitymgr.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.JdbcTempalteFactory;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@Component("JdbcTempalteFactory")
public class JdbcTemplateFactoryImpl implements JdbcTempalteFactory {

    private Map<String, NamedParameterJdbcTemplate> jdbcTempates = new HashMap<>();

    @Autowired
    private PlaymakerTenantEntityMgr tenantEntityMgr;

    @Value("${playmaker.jdbc.pool.min.size}")
    private int minPoolSize;
    @Value("${playmaker.jdbc.pool.max.size}")
    private int maxPoolSize;
    @Value("${playmaker.jdbc.pool.max.idle}")
    private int maxPoolIdleTime;
    @Value("${playmaker.jdbc.pool.max.checkout}")
    private int maxPoolCheckoutTime;

    public NamedParameterJdbcTemplate getTemplate(String tenantName) {

        NamedParameterJdbcTemplate template = jdbcTempates.get(tenantName);
        if (template == null) {
            synchronized (jdbcTempates) {
                template = jdbcTempates.get(tenantName);
                if (template == null) {
                    ComboPooledDataSource cpds = getDataSource(tenantName);
                    template = new NamedParameterJdbcTemplate(cpds);
                    jdbcTempates.put(tenantName, template);
                }
            }
        }
        return template;
    }

    public void removeTemplate(String tenantName) {

        synchronized (jdbcTempates) {
            jdbcTempates.remove(tenantName);
        }
    }

    private ComboPooledDataSource getDataSource(String tenantName) {

        PlaymakerTenant tenant = tenantEntityMgr.findByTenantName(tenantName);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_22001, new String[] { tenantName });
        }
        try {
            ComboPooledDataSource cpds = new ComboPooledDataSource();
            cpds.setDriverClass(tenant.getJdbcDriver());
            cpds.setUser(tenant.getJdbcUserName());
            cpds.setPassword(tenant.getJdbcPassword());
            cpds.setJdbcUrl(tenant.getJdbcUrl());

            cpds.setMinPoolSize(minPoolSize);
            cpds.setMaxPoolSize(maxPoolSize);
            cpds.setMaxIdleTime(maxPoolIdleTime);
            cpds.setCheckoutTimeout(maxPoolCheckoutTime);

            return cpds;

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_22000, ex, new String[] { tenantName });
        }

    }
}
