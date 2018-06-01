package com.latticeengines.playmaker.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmaker.service.RecommendationCleanupService;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component("recommendationCleanupService")
public class RecommendationCleanupServiceImpl implements RecommendationCleanupService {

    private static final Logger log = LoggerFactory.getLogger(RecommendationCleanupServiceImpl.class);

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
            log.info(String.format("Trying to cleanup recommendations for tenant: %s", tenant));

            MultiTenantContext.setTenant(tenant);

            List<String> deletedPlayIdsForCleanup = playProxy
                    .getDeletedPlayIds(MultiTenantContext.getCustomerSpace().toString(), true);

            if (CollectionUtils.isNotEmpty(deletedPlayIdsForCleanup)) {
                deletedPlayIdsForCleanup.stream().forEach(id -> lpiPMRecommendation.cleanupRecommendations(id));
            }
            log.info(String.format("Completed cleanup recommendations for tenant: %s", tenant));
        } catch (Exception ex) {
            log.error(String.format("Failed to cleanup recommendations for tenant: %s", tenant), ex);
        } finally {
            MultiTenantContext.setTenant(null);
        }
    }
}
