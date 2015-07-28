package com.latticeengines.playmaker.entitymgr.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.digest.DigestUtils;
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

    private Map<String, TemplateInfo> jdbcTempates = new ConcurrentHashMap<>();

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

        PlaymakerTenant tenant = tenantEntityMgr.findByTenantName(tenantName);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_22001, new String[] { tenantName });
        }

        boolean hasCreatedNew = false;
        TemplateInfo templateInfo = jdbcTempates.get(tenantName);
        if (templateInfo == null) {
            synchronized (jdbcTempates) {
                templateInfo = jdbcTempates.get(tenantName);
                if (templateInfo == null) {
                    templateInfo = getTemplateInfo(tenant);
                    jdbcTempates.put(tenantName, templateInfo);
                }
            }
        } else if (isHashChanged(tenant, templateInfo)) {
            if (!hasCreatedNew) {
                synchronized (jdbcTempates) {
                    if (!hasCreatedNew) {
                        templateInfo = getTemplateInfo(tenant);
                        jdbcTempates.put(tenantName, templateInfo);
                        hasCreatedNew = true;
                    }
                }
            }
        }

        return templateInfo.template;
    }

    public void removeTemplate(String tenantName) {

        synchronized (jdbcTempates) {
            jdbcTempates.remove(tenantName);
        }
    }

    public boolean isHashChanged(PlaymakerTenant tenant, TemplateInfo templateInfo) {
        if (templateInfo == null || templateInfo.hash == null) {
            return false;
        }
        return !Arrays.equals(templateInfo.hash, getHash(tenant));
    }

    private TemplateInfo getTemplateInfo(PlaymakerTenant tenant) {

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

            NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(cpds);
            byte[] hash = getHash(tenant);
            TemplateInfo templateInfo = new TemplateInfo(template, hash);
            return templateInfo;

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_22000, ex, new String[] { tenant.getTenantName() });
        }
    }

    private byte[] getHash(PlaymakerTenant tenant) {
        String hashStr = tenant.getJdbcDriver() + tenant.getJdbcUrl() + tenant.getJdbcUserName()
                + tenant.getJdbcPassword();
        return DigestUtils.md5(hashStr);
    }

    private static class TemplateInfo {

        private NamedParameterJdbcTemplate template;
        private byte[] hash;

        public TemplateInfo(NamedParameterJdbcTemplate template, byte[] hash) {
            this.template = template;
            this.hash = hash;
        }

    }

}
