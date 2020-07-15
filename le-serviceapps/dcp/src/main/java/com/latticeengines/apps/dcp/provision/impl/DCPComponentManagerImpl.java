package com.latticeengines.apps.dcp.provision.impl;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.provision.DCPComponentManager;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.monitor.tracing.TracingTags;
import com.latticeengines.monitor.util.TracingUtils;

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

@Component("dcpComponentManager")
public class DCPComponentManagerImpl implements DCPComponentManager {

    private static final Logger log = LoggerFactory.getLogger(DCPComponentManagerImpl.class);


    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private BatonService batonService;

    @Override
    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();
        String customerSpace = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
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
            log.info("Provisioning dcp component for tenant {}", customerSpace);
            provisionSpan.log("Provisioning dcp component for tenant " + customerSpace);
        } finally {
            TracingUtils.finish(provisionSpan);
        }
    }

    private Scope startProvisionSpan(SpanContext parentContext, String tenantId, long startTimeStamp) {
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("DCPComponent Bootstrap - " + tenantId) //
                .addReference(References.FOLLOWS_FROM, parentContext) //
                .withTag(TracingTags.TENANT_ID, tenantId) //
                .withStartTimestamp(startTimeStamp) //
                .start();
        return tracer.activateSpan(span);
    }

    @Override
    public void discardTenant(String customerSpace) {
    }

}
