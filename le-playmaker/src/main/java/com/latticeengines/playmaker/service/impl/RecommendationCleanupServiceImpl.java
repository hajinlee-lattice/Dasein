package com.latticeengines.playmaker.service.impl;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmaker.service.RecommendationCleanupService;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component("recommendationCleanupService")
public class RecommendationCleanupServiceImpl implements RecommendationCleanupService {

    private static final Logger log = LoggerFactory.getLogger(RecommendationCleanupServiceImpl.class);

    @Value("${playmaker.recommendations.years.keep:2}")
    private Double YEARS_TO_KEEP_RECOMMENDATIONS;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private LpiPMRecommendation lpiPMRecommendation;

    @Override
    public void cleanup() {

        List<Tenant> tenants = tenantEntityMgr.findAll();
        if (CollectionUtils.isNotEmpty(tenants)) {
            tenants.stream() //
                    .forEach(tenant -> cleanupForTenant(tenant));
        }
    }

    private void cleanupForTenant(Tenant tenant) {
        try {
            MultiTenantContext.setTenant(tenant);
            cleanupRecommendationsDueToDeletedPlays(tenant);
            cleanupVeryOldRecommendations(tenant);
        } finally {
            MultiTenantContext.setTenant(null);
        }
    }

    private void cleanupVeryOldRecommendations(Tenant tenant) {
        try {
            Date cutoffDate = PlaymakerUtils.dateFromEpochSeconds(System.currentTimeMillis() / 1000L
                    - TimeUnit.DAYS.toSeconds(Math.round(365 * YEARS_TO_KEEP_RECOMMENDATIONS)));
            lpiPMRecommendation.cleanupOldRecommendationsBeforeCutoffDate(cutoffDate);
        } catch (Exception ex) {
            log.error(String.format("Failed to cleanup very old recommendations for tenant: %s", tenant), ex);
        }
    }

    private void cleanupRecommendationsDueToDeletedPlays(Tenant tenant) {
        try {
            List<String> deletedPlayIdsForCleanup = playProxy
                    .getDeletedPlayIds(MultiTenantContext.getCustomerSpace().toString(), true);

            if (CollectionUtils.isNotEmpty(deletedPlayIdsForCleanup)) {
                deletedPlayIdsForCleanup.stream().forEach(id -> lpiPMRecommendation.cleanupRecommendations(id));
            }
        } catch (Exception ex) {
            log.error(String.format("Failed to cleanup recommendations for tenant: %s", tenant), ex);
        }
    }
}
