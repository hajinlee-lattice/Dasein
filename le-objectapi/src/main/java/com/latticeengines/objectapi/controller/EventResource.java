package com.latticeengines.objectapi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.service.EventQueryService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import reactor.core.publisher.Mono;

@Api(value = "events", description = "REST resource for modeling events")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/event")
public class EventResource {
    private final EventQueryService eventQueryService;

    @Autowired
    public EventResource(EventQueryService eventQueryService) {
        this.eventQueryService = eventQueryService;
    }

    @PostMapping(value = "/count/scoring")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Mono<Long> getScoringCount(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        final Tenant tenant = MultiTenantContext.getTenant();
        return Mono.fromCallable(() -> {
            MultiTenantContext.setTenant(tenant);
            return eventQueryService.getScoringCount(frontEndQuery, version);
        });
    }

    @PostMapping(value = "/count/training")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Mono<Long> getTrainingCount(@PathVariable String customerSpace,
            @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        final Tenant tenant = MultiTenantContext.getTenant();
        return Mono.fromCallable(() -> {
            MultiTenantContext.setTenant(tenant);
            return eventQueryService.getTrainingCount(frontEndQuery, version);
        });
    }

    @PostMapping(value = "/count/event")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public Mono<Long> getEventCount(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        final Tenant tenant = MultiTenantContext.getTenant();
        return Mono.fromCallable(() -> {
            MultiTenantContext.setTenant(tenant);
            return eventQueryService.getEventCount(frontEndQuery, version);
        });
    }

    @PostMapping(value = "/data/scoring")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Mono<DataPage> getScoringTuples(@PathVariable String customerSpace,
                                           @RequestBody EventFrontEndQuery frontEndQuery,
                                           @RequestParam(value = "version", required = false) DataCollection.Version version) {
        final Tenant tenant = MultiTenantContext.getTenant();
        return Mono.fromCallable(() -> {
            MultiTenantContext.setTenant(tenant);
            return eventQueryService.getScoringTuples(frontEndQuery, version);
        });
    }

    @PostMapping(value = "/data/training")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Mono<DataPage> getTrainingTuples(@PathVariable String customerSpace,
            @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        final Tenant tenant = MultiTenantContext.getTenant();
        return Mono.fromCallable(() -> {
            MultiTenantContext.setTenant(tenant);
            return eventQueryService.getTrainingTuples(frontEndQuery, version);
        });
    }

    @PostMapping(value = "/data/event")
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public Mono<DataPage> getEventTuples(@PathVariable String customerSpace,
            @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        final Tenant tenant = MultiTenantContext.getTenant();
        return Mono.fromCallable(() -> {
            MultiTenantContext.setTenant(tenant);
            return eventQueryService.getEventTuples(frontEndQuery, version);
        });
    }

}
