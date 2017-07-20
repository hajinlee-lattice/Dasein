package com.latticeengines.playmaker.entitymgr.impl;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmaker.dao.PlaymakerDBVersionDao;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;
import com.latticeengines.playmaker.dao.impl.PlaymakerDBVersionDaoImpl;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImpl;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImplV710;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImplV740;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImplV750;
import com.latticeengines.playmaker.entitymgr.PlaymakerDaoFactory;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("daoFactory")
public class PlaymakerDaoFactoryImpl implements PlaymakerDaoFactory {

    private static final Logger log = LoggerFactory.getLogger(PlaymakerDaoFactoryImpl.class);

    @Autowired
    private JdbcTemplateFactoryImpl templateFactory;

    @Autowired
    private BatonService batonService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    @Qualifier(value = "lpiPMRecommendationDaoAdapter")
    private PlaymakerRecommendationDao lpiPlaymakerRecommendationDao;

    private Map<String, Class<? extends PlaymakerRecommendationDao>> versionDaoMap;

    @PostConstruct
    public void postConstruct() {
        versionDaoMap = new TreeMap<>(Collections.reverseOrder());
        versionDaoMap.put(normalizedVer("7.4.0"), PlaymakerRecommendationDaoImplV740.class);
        versionDaoMap.put(normalizedVer("7.5.0"), PlaymakerRecommendationDaoImplV750.class);
        versionDaoMap.put(normalizedVer("7.10.0"), PlaymakerRecommendationDaoImplV710.class);
    }

    @Override
    public PlaymakerRecommendationDao getRecommendationDao(String tenantName) {
        if (isLpiBasedPlaymakerEnabledForTenant(tenantName)) {
            return lpiPlaymakerRecommendationDao;
        } else {
            PlaymakerRecommendationDao defaultDao = null;
            NamedParameterJdbcTemplate namedJdbcTemplate = templateFactory.getTemplate(tenantName);
            defaultDao = new PlaymakerRecommendationDaoImpl(namedJdbcTemplate);

            try {
                PlaymakerDBVersionDao versionDao = new PlaymakerDBVersionDaoImpl(namedJdbcTemplate);
                String version = versionDao.getDBVersion();
                String normalizedVer = normalizedVer(version);

                return findDao(namedJdbcTemplate, normalizedVer, defaultDao);

            } catch (Exception ex) {
                log.warn(String.format("Failed to get Dao! tenantName=%s", tenantName), ex);
            }
            return defaultDao;
        }
    }

    private boolean isLpiBasedPlaymakerEnabledForTenant(String tenantName) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantName);
        try {
            if (batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_TALKING_POINTS)) {
                Tenant tenant = tenantEntityMgr.findByTenantId(tenantName);
                MultiTenantContext.setTenant(tenant);
                return true;
            }
        } catch (Exception ex) {
            boolean isIgnorableException = false;
            if (ex instanceof NoNodeException) {
                // ignore this error as old PL tenant may not have ZK path
                // created on LPI side. This will then return false by default
                log.debug("Ignoring: " + ex.getMessage());
                isIgnorableException = true;
            } else {
                Throwable th = ex.getCause();
                while (th != null) {
                    if (th instanceof NoNodeException) {
                        // ignore this error as old PL tenant may not have ZK
                        // path
                        // created on LPI side. This will then return false by
                        // default
                        log.debug("Ignoring: " + th.getMessage());
                        isIgnorableException = true;
                        break;
                    }
                }

                if (!isIgnorableException) {
                    throw ex;
                }
            }
        }
        return false;
    }

    PlaymakerRecommendationDao findDao(NamedParameterJdbcTemplate namedJdbcTemplate, String normalizedVer,
            PlaymakerRecommendationDao defaultDao) throws Exception {
        for (Map.Entry<String, Class<? extends PlaymakerRecommendationDao>> entry : versionDaoMap.entrySet()) {
            int result = normalizedVer.compareTo(entry.getKey());
            if (result >= 0) {
                Class<?> daoClass = entry.getValue();
                return (PlaymakerRecommendationDao) daoClass.getConstructor(NamedParameterJdbcTemplate.class)
                        .newInstance(namedJdbcTemplate);
            }
        }
        return defaultDao;
    }

    String normalizedVer(String version) {
        StringBuilder builder = new StringBuilder();
        String[] tokens = version.split("[.]");
        for (String token : tokens) {
            builder.append(StringUtils.leftPad(token, 3, '0'));
        }

        return builder.toString();
    }

}
