package com.latticeengines.playmaker.entitymgr.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.JdbcTemplateFactory;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@Component("jdbcTemplateFactory")
public class JdbcTemplateFactoryImpl implements JdbcTemplateFactory {

    private static final Log log = LogFactory.getLog(JdbcTemplateFactoryImpl.class);

    private Map<String, TemplateInfo> jdbcTemplates = new ConcurrentHashMap<>();

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

    @Value("${playmaker.datasource.user}")
    private String dataSouceUser;

    @Value("${playmaker.datasource.password.encrypted}")
    private String dataSoucePassword;

    @Override
    public NamedParameterJdbcTemplate getTemplate(String tenantName) {

        PlaymakerTenant tenant = tenantEntityMgr.findByTenantName(tenantName);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_22001, new String[] { tenantName });
        }

        boolean hasCreatedNew = false;
        TemplateInfo templateInfo = jdbcTemplates.get(tenantName);
        if (templateInfo == null) {
            synchronized (jdbcTemplates) {
                templateInfo = jdbcTemplates.get(tenantName);
                if (templateInfo == null) {
                    templateInfo = getTemplateInfo(tenant);
                    jdbcTemplates.put(tenantName, templateInfo);
                }
            }
        } else if (isHashChanged(tenant, templateInfo)) {
            if (!hasCreatedNew) {
                synchronized (jdbcTemplates) {
                    if (!hasCreatedNew) {
                        removeTemplate(tenantName);
                        templateInfo = getTemplateInfo(tenant);
                        jdbcTemplates.put(tenantName, templateInfo);
                        hasCreatedNew = true;
                    }
                }
            }
        }

        return templateInfo.template;
    }

    public void removeTemplate(String tenantName) {

        synchronized (jdbcTemplates) {
            TemplateInfo tempInfo = jdbcTemplates.remove(tenantName);
            if (tempInfo != null) {
                try {
                    tempInfo.cpds.close();
                } catch (Exception ex) {
                    log.warn("Can not close the data source for tenant=" + tenantName, ex);
                }
            }
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
            if (StringUtils.isBlank(tenant.getJdbcUserName())) {
                cpds.setUser(dataSouceUser);
            } else {
                cpds.setUser(tenant.getJdbcUserName());
            }
            if (StringUtils.isBlank(tenant.getJdbcPassword())) {
                cpds.setPassword(dataSoucePassword);
            } else {
                cpds.setPassword(tenant.getJdbcPassword());
            }
            cpds.setJdbcUrl(tenant.getJdbcUrl());

            cpds.setMinPoolSize(minPoolSize);
            cpds.setMaxPoolSize(maxPoolSize);
            cpds.setMaxIdleTime(maxPoolIdleTime);
            cpds.setCheckoutTimeout(maxPoolCheckoutTime);
            cpds.setBreakAfterAcquireFailure(true);

            NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(cpds);
            byte[] hash = getHash(tenant);
            TemplateInfo templateInfo = new TemplateInfo(template, hash, cpds);
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
        private ComboPooledDataSource cpds;

        public TemplateInfo(NamedParameterJdbcTemplate template, byte[] hash, ComboPooledDataSource cpds) {
            this.template = template;
            this.hash = hash;
            this.cpds = cpds;
        }

    }

}
