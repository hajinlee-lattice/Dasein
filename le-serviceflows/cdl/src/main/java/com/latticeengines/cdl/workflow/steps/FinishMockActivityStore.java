package com.latticeengines.cdl.workflow.steps;


import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MockActivityStoreConfiguration;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("finishMockActivityStore")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FinishMockActivityStore extends BaseWorkflowStep<MockActivityStoreConfiguration> {

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Resource(name = "localCacheService")
    private CacheService localCacheService;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        clearCache(customerSpace.toString());
        try {
            // wait for local cache clean up
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            // ignore
        }
        servingStoreProxy.getDecoratedMetadataFromCache(customerSpace.toString(), BusinessEntity.Account);
    }

    public void clearCache(String customerSpace) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(tenantId, CacheName.getCdlCacheGroup());
        localCacheService.refreshKeysByPattern(tenantId, CacheName.getCdlLocalCacheGroup());
    }

}
