package com.latticeengines.pls.controller.datacollection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entities", description = "REST resource for serving data about multiple entities")
@RestController
@RequestMapping("/entities")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class EntityResource extends BaseFrontEndEntityResource {

    private static final Logger log = LoggerFactory.getLogger(EntityResource.class);

    private ExecutorService executorService = //
            ThreadPoolUtils.getFixedSizeThreadPool("entity-count", BusinessEntity.COUNT_ENTITIES.size());

    @Inject
    public EntityResource(EntityProxy entityProxy, SegmentProxy segmentProxy, DataCollectionProxy dataCollectionProxy,
                          GraphDependencyToUIActionUtil graphDependencyToUIActionUtil) {
        super(entityProxy, segmentProxy, dataCollectionProxy, graphDependencyToUIActionUtil);

    }

    @RequestMapping(value = "/counts", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Map<BusinessEntity, Long> getCounts(@RequestBody(required = false) FrontEndQuery frontEndQuery) {
        Map<BusinessEntity, Future<Long>> futures = new HashMap<>();
        Tenant tenant = MultiTenantContext.getTenant();
        for (BusinessEntity entity: BusinessEntity.COUNT_ENTITIES) {
            futures.put(entity, executorService.submit(new CountFetcher(tenant, entity, frontEndQuery)));
        }
        Map<BusinessEntity, Long> counts = new HashMap<>();
        for (BusinessEntity entity: BusinessEntity.COUNT_ENTITIES) {
            try {
                Long count = futures.get(entity).get();
                counts.put(entity, count);
            } catch (Exception e) {
                log.warn("Failed to get count of " + entity, e);
            }
        }
        return counts;
    }

    @Override
    BusinessEntity getMainEntity() {
        return null;
    }

    private class CountFetcher implements Callable<Long> {

        private final Tenant tenant;
        private final BusinessEntity entity;
        private final FrontEndQuery frontEndQuery;

        CountFetcher(Tenant tenant, BusinessEntity entity, FrontEndQuery frontEndQuery) {
            this.tenant = tenant;
            this.entity = entity;
            this.frontEndQuery = JsonUtils.deserialize(JsonUtils.serialize(frontEndQuery), FrontEndQuery.class);
        }

        @Override
        public Long call() {
            MultiTenantContext.setTenant(tenant);
            String tenantId = CustomerSpace.shortenCustomerSpace(tenant.getId());
            Long count = getCount(tenantId, frontEndQuery, entity);
            if (count != null) {
                return count;
            } else {
                return 0L;
            }
        }

    }

    @Override
    List<Lookup> getDataLookups() {
        throw new UnsupportedOperationException("Do not support data query.");
    }

}
