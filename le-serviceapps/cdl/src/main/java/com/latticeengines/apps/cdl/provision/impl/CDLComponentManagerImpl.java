package com.latticeengines.apps.cdl.provision.impl;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.provision.CDLComponentManager;
import com.latticeengines.apps.cdl.service.AtlasSchedulingService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DropBoxCrossTenantService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.DataUnitCrossTenantService;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

@Component
public class CDLComponentManagerImpl implements CDLComponentManager {

    private static final Logger log = LoggerFactory.getLogger(CDLComponentManagerImpl.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private DataUnitCrossTenantService dataUnitCrossTenantService;

    @Inject
    private DropBoxCrossTenantService dropBoxCrossTenantService;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private AttributeSetEntityMgr attributeSetEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private AtlasSchedulingService atlasSchedulingService;

    @Inject
    private BatonService batonService;

    @Override
    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();
        String customerSpace = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
        log.info(String.format("Provisioning tenant %s", customerSpace));
        Map<String, String> parentCtxMap = null;
        try {
            String contextStr = configDir.get("/TracingContext").getDocument().getData();
            Map<?, ?> rawMap = JsonUtils.deserialize(contextStr, Map.class);
            parentCtxMap = JsonUtils.convertMap(rawMap, String.class, String.class);
        } catch (Exception e) {
            log.warn("no tracing context node exist {}.", e.getMessage());
        }
        long start = System.currentTimeMillis() * 1000;
        Tracer tracer = GlobalTracer.get();
        SpanContext parentCtx = TracingUtils.getSpanContext(parentCtxMap);
        Span provisionSpan = null;
        try (Scope scope = startProvisionSpan(parentCtx, customerSpace, start)) {
            provisionSpan = tracer.activeSpan();
            provisionSpan.log("Provisioning cdl component for tenant " + customerSpace);
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
            MultiTenantContext.setTenant(tenant);
            dataCollectionEntityMgr.createDefaultCollection();
            DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
            log.info("Initialized data collection " + dataFeed.getDataCollection().getName());
            provisionSpan.log("Initialized data collection " + dataFeed.getDataCollection().getName());
            provisionDropBox(space);
            attributeSetEntityMgr.createDefaultAttributeSet();
            if (!batonService.hasProduct(CustomerSpace.parse(customerSpace), LatticeProduct.DCP)) {
                if (!batonService.isEnabled(CustomerSpace.parse(customerSpace), LatticeFeatureFlag.ENABLE_ENTITY_MATCH)) {
                    log.info("Create Default System for tenant: " + space.toString());
                    s3ImportSystemService.createDefaultImportSystem(space.toString());
                    dropBoxService.createTenantDefaultFolder(space.toString());
                }
                if (configDir.get("/ExportCronExpression") != null) {
                    String exportCron = configDir.get("/ExportCronExpression").getDocument().getData();
                    log.info(String.format("Export Cron for tenant %s is: %s", customerSpace, exportCron));
                    atlasSchedulingService.createOrUpdateSchedulingByType(customerSpace, exportCron, AtlasScheduling.ScheduleType.Export);
                }
                if (configDir.get("/PAScheduleNowCronExpression") != null) {
                    String paCron = configDir.get("/PAScheduleNowCronExpression").getDocument().getData();
                    log.info(String.format("PA schedule now Cron for tenant %s is: %s", customerSpace, paCron));
                    atlasSchedulingService.createOrUpdateSchedulingByType(customerSpace, paCron, AtlasScheduling.ScheduleType.PA);
                }
            }
        } finally {
            TracingUtils.finish(provisionSpan);
        }
    }

    private Scope startProvisionSpan(SpanContext parentContext, String tenantId, long startTimeStamp) {
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("CDLComponent Bootstrap") //
                .addReference(References.FOLLOWS_FROM, parentContext) //
                .withTag(TracingTags.TENANT_ID, tenantId) //
                .withStartTimestamp(startTimeStamp) //
                .start();
        return tracer.activateSpan(span);
    }

    @Override
    public void discardTenant(String customerSpace) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        attrConfigEntityMgr.cleanupTenant(tenantId);
        dataUnitCrossTenantService.cleanupByTenant(customerSpace);
        dropBoxCrossTenantService.delete(customerSpace);
    }

    private void provisionDropBox(CustomerSpace customerSpace) {
        DropBox dropBox = dropBoxService.create();
        log.info("Created dropbox " + dropBox.getDropBox() + " for " + customerSpace.getTenantId());
    }

}
