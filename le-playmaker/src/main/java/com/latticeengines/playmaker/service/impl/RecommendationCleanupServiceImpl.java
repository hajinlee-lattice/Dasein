package com.latticeengines.playmaker.service.impl;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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

    @Value("${playmaker.expired.tenants.recommendations.years.keep:1}")
    private Long YEARS_TO_KEEP_RECOMMENDATIONS_FOR_EXPIRED_TENANTS;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private LpiPMRecommendation lpiPMRecommendation;

    @Override
    public void cleanup() {

        List<Tenant> tenants = tenantEntityMgr.findAll();
        if (CollectionUtils.isNotEmpty(tenants)) {
            tenants.stream() //
                    .filter(tenant -> batonService.isEnabled(CustomerSpace.parse(tenant.getId()),
                            LatticeFeatureFlag.PLAYBOOK_MODULE))
                    .forEach(tenant -> cleanupForTenant(tenant));
        }
        cleanupRecommendationForChurnedCustomers();
    }

    private void cleanupForTenant(Tenant tenant) {
        try {
            MultiTenantContext.setTenant(tenant);
            cleanupRecommendationsDueToDeletedPlays();
            cleanupVeryOldRecommendations();
        } finally {
            MultiTenantContext.setTenant(null);
        }
    }

    int cleanupVeryOldRecommendations() {
        try {
            Date cutoffDate = PlaymakerUtils.dateFromEpochSeconds(System.currentTimeMillis() / 1000L
                    - TimeUnit.DAYS.toSeconds(Math.round(365 * YEARS_TO_KEEP_RECOMMENDATIONS)));
            return lpiPMRecommendation.cleanupOldRecommendationsBeforeCutoffDate(cutoffDate);
        } catch (Exception ex) {
            log.error(String.format("Failed to cleanup very old recommendations for tenant: %s",
                    MultiTenantContext.getTenant().getId()), ex);
            return 0;
        }
    }

    int cleanupRecommendationsDueToDeletedPlays() {
        return cleanupRecommendationsDueToDeletedPlays(null);
    }

    int cleanupRecommendationsDueToDeletedPlays(List<String> deletedPlayIdsForCleanup) {
        String tenantId = MultiTenantContext.getTenant().getId();
        int totalDeletedCount = 0;
        try {
            if (CollectionUtils.isEmpty(deletedPlayIdsForCleanup)) {
                deletedPlayIdsForCleanup = playProxy.getDeletedPlayIds(tenantId, true);
            }

            if (CollectionUtils.isNotEmpty(deletedPlayIdsForCleanup)) {
                long timestamp = System.currentTimeMillis();
                log.info(String.format("Initiating cleanup for recommendations of %d deleted plays for tenant %s", //
                        deletedPlayIdsForCleanup.size(), tenantId));
                totalDeletedCount = deletedPlayIdsForCleanup.stream()
                        .map(id -> lpiPMRecommendation.cleanupRecommendations(id)).reduce(0, (x, y) -> x + y);
                log.info(String.format(
                        "Finished cleanup for recommendations of %d deleted plays for tenant %s in %d milliseconds", //
                        deletedPlayIdsForCleanup.size(), tenantId, (System.currentTimeMillis() - timestamp)));
            }
        } catch (Exception ex) {
            log.error(String.format("Failed to cleanup recommendations for tenant: %s", tenantId), ex);
        }

        return totalDeletedCount;
    }

    void cleanupRecommendationForChurnedCustomers() {
        int softDeleted = cleanupRecommendationDueToExpiredTenants(false);
        int hardDeleted = cleanupRecommendationDueToExpiredTenants(true);
        log.info("Deleting recommendations for expired tenants, Soft delete: " + softDeleted + " Hard delete: "
                + hardDeleted);
    }

    int cleanupRecommendationDueToExpiredTenants(boolean isHardDelete) {
        int totalDeletedCount = 0;
        Date expiredDate = Date.from(Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
                .minusYears(YEARS_TO_KEEP_RECOMMENDATIONS_FOR_EXPIRED_TENANTS).toInstant());
        List<Long> tenantIdsForCleanup = getChurnedTenants();
        try {
            if (CollectionUtils.isNotEmpty(tenantIdsForCleanup)) {
                totalDeletedCount = tenantIdsForCleanup.stream().map(
                        id -> lpiPMRecommendation.cleanupRecommendationsForChurnedTenant(id, expiredDate, isHardDelete))
                        .reduce(0, (x, y) -> x + y);
            }
        } catch (Exception ex) {
            log.error(String.format("Failed to cleanup recommendations for Expired tenants"), ex);
        }
        return totalDeletedCount;
    }

    private List<Long> getChurnedTenants() {

        List<Long> existTenants = tenantEntityMgr.getAllTenantPid();
        List<Long> recommendationTenants = lpiPMRecommendation.getAllTenantIdsFromRecommendation();

        recommendationTenants.removeAll(existTenants);
        log.info(String.format("Find %d churned tenants from Recommendation: ", recommendationTenants.size())
                + Arrays.toString(existTenants.toArray()));

        return recommendationTenants;
    }
}
