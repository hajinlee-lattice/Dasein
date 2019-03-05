package com.latticeengines.playmaker.entitymgr.impl;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.playmaker.PlaymakerSyncLookupSource;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmaker.dao.PlaymakerDBVersionDao;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;
import com.latticeengines.playmaker.dao.impl.PlaymakerDBVersionDaoImpl;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImpl;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImplV710;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImplV740;
import com.latticeengines.playmaker.dao.impl.PlaymakerRecommendationDaoImplV750;
import com.latticeengines.playmaker.entitymgr.PlaymakerDaoFactory;

@Component("daoFactory")
public class PlaymakerDaoFactoryImpl implements PlaymakerDaoFactory {

    private static final Logger log = LoggerFactory.getLogger(PlaymakerDaoFactoryImpl.class);

    @Inject
    private JdbcTemplateFactoryImpl templateFactory;

    @Inject
    private BatonService batonService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Resource(name = "lpiPMRecommendationDaoAdapter")
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
    public PlaymakerRecommendationDao getRecommendationDao(String tenantName, String lookupSource) {
        if (shouldUseLpiBasedPlaymaker(tenantName, lookupSource)) {
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

    private boolean shouldUseLpiBasedPlaymaker(String tenantName, String lookupSource) {
        PlaymakerSyncLookupSource playmakerSyncLookupSource = getPlaymakerSyncLookupSource(lookupSource);

        // if playmakerSyncLookupSource is DECIDED_BY_FEATURE_FLAG then use
        // tenant level feature flags to decide lookup source otherwise use LPI
        // of playmaker as indicated by loop source enum passed by caller

        boolean shouldUseLpi = false;

        if (playmakerSyncLookupSource == PlaymakerSyncLookupSource.DECIDED_BY_FEATURE_FLAG) {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantName);
            try {
                if (batonService.isEnabled(customerSpace, LatticeFeatureFlag.ENABLE_LPI_PLAYMAKER)) {
                    shouldUseLpi = true;
                }
            } catch (Exception ex) {
                boolean isIgnorableException = false;
                if (ex instanceof NoNodeException) {
                    // ignore this error as old PL tenant may not have ZK path
                    // created on LPI side. This will then return false by
                    // default
                    log.debug("Ignoring: " + ex.getMessage());
                    isIgnorableException = true;
                } else {
                    Throwable th = ex.getCause();
                    while (th != null) {
                        if (th instanceof NoNodeException) {
                            // ignore this error as old PL tenant may not have
                            // ZK
                            // path
                            // created on LPI side. This will then return false
                            // by
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
        } else {
            shouldUseLpi = playmakerSyncLookupSource == PlaymakerSyncLookupSource.V2;
        }

        if (shouldUseLpi) {
            Tenant tenant = tenantEntityMgr.findByTenantId(tenantName);
            MultiTenantContext.setTenant(tenant);
        }

        return shouldUseLpi;
    }

    private PlaymakerSyncLookupSource getPlaymakerSyncLookupSource(String lookupSource) {
        PlaymakerSyncLookupSource playmakerSyncLookupSource = PlaymakerSyncLookupSource.DECIDED_BY_FEATURE_FLAG;
        if (StringUtils.isNotBlank(lookupSource)) {
            playmakerSyncLookupSource = PlaymakerSyncLookupSource.valueOf(lookupSource.trim());
        }
        return playmakerSyncLookupSource;
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
