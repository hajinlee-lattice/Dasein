package com.latticeengines.apps.cdl.controller;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import reactor.core.publisher.Flux;

@Api(value = "serving store", description = "REST resource for serving stores")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/servingstore")
public class ServingStoreResource {

    private static final Logger log = LoggerFactory.getLogger(ServingStoreResource.class);

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private DataCollectionService dataCollectionService;

    @GetMapping(value = "/{entity}/decoratedmetadata")
    @ResponseBody
    @ApiOperation(value = "Get decorated serving store metadata")
    public Flux<ColumnMetadata> getDecoratedMetadata( //
            @PathVariable String customerSpace, @PathVariable BusinessEntity entity, //
            @RequestParam(name = "groups", required = false) List<ColumnSelection.Predefined> groups, //
            @RequestParam(name = "version", required = false) DataCollection.Version version) {
        return getFlux(customerSpace, entity, version, groups);
    }

    private Flux<ColumnMetadata> getFlux(String customerSpace, BusinessEntity entity, DataCollection.Version version,
            List<ColumnSelection.Predefined> groups) {
        AtomicLong timer = new AtomicLong();
        AtomicLong counter = new AtomicLong();
        Flux<ColumnMetadata> flux;
        if (version == null) {
            flux = servingStoreService
                    .getFullyDecoratedMetadata(entity, dataCollectionService.getActiveVersion(customerSpace))
                    .sequential();
        } else {
            flux = servingStoreService.getFullyDecoratedMetadata(entity, version).sequential();
        }
        flux = flux //
                .doOnSubscribe(s -> {
                    timer.set(System.currentTimeMillis());
                    log.info("Start serving decorated metadata for " + customerSpace + ":" + entity);
                }) //
                .doOnNext(cm -> counter.getAndIncrement()) //
                .doOnComplete(() -> {
                    long duration = System.currentTimeMillis() - timer.get();
                    log.info("Finished serving decorated metadata for " + counter.get() + " attributes from "
                            + customerSpace + ":" + entity + " TimeElapsed=" + duration + " msec");
                });
        Set<ColumnSelection.Predefined> filterGroups = new HashSet<>();
        if (CollectionUtils.isNotEmpty(groups)) {
            filterGroups.addAll(groups);
        }
        if (CollectionUtils.isNotEmpty(filterGroups)) {
            flux = flux.filter(cm -> filterGroups.stream().anyMatch(cm::isEnabledFor));
        }
        return flux;
    }

    @GetMapping(value = "/systemmetadata")
    @ResponseBody
    @ApiOperation(value = "Get system metadata attributes")
    public Flux<ColumnMetadata> getSystemMetadataAttrs( //
            @PathVariable String customerSpace, //
            @RequestParam(name = "entity", required = true) BusinessEntity entity, //
            @RequestParam(name = "version", required = false) DataCollection.Version version) {
        return getSystemMetadataAttrFlux(customerSpace, entity, version);
    }

    @GetMapping(value = "/new-modeling")
    @ResponseBody
    @ApiOperation(value = "Get attributes that are enabled for first iteration modeling")
    public Flux<ColumnMetadata> getNewModelingAttrs( //
            @PathVariable String customerSpace, //
            @RequestParam(name = "entity", required = false, defaultValue = "Account") BusinessEntity entity, //
            @RequestParam(name = "version", required = false) DataCollection.Version version) {
        log.info(String.format("Get new modeling attributes for %s with entity %s", customerSpace, entity));
        if (!BusinessEntity.MODELING_ENTITIES.contains(entity)) {
            throw new UnsupportedOperationException(String.format("%s is not supported for modeling.", entity));
        }
        Flux<ColumnMetadata> flux = getFlux(customerSpace, entity, version,
                Collections.singletonList(ColumnSelection.Predefined.Model));
        flux = flux.map(cm -> {
            cm.setApprovedUsageList(Collections.singletonList(ApprovedUsage.MODEL_ALLINSIGHTS));
            if (cm.getTagList() == null || (cm.getTagList() != null && !cm.getTagList().contains(Tag.EXTERNAL))) {
                cm.setTagList(Collections.singletonList(Tag.INTERNAL));
            }
            return cm;
        });
        return flux;
    }

    @GetMapping(value = "/allow-modeling")
    @ResponseBody
    @ApiOperation(value = "Get attributes that are allowed for modeling")
    public Flux<ColumnMetadata> getAllowedModelingAttrs( //
            @PathVariable String customerSpace, //
            @RequestParam(name = "entity", required = false, defaultValue = "Account") BusinessEntity entity, //
            @RequestParam(name = "version", required = false) DataCollection.Version version, //
            @RequestParam(name = "all-customer-attrs", required = false) Boolean allCustomerAttrs) {
        log.info(String.format("Get allow modeling attributes for %s with entity %s", customerSpace, entity));
        if (!BusinessEntity.MODELING_ENTITIES.contains(entity)) {
            throw new UnsupportedOperationException(String.format("%s is not supported for modeling.", entity));
        }
        Flux<ColumnMetadata> flux = getSystemMetadataAttrFlux(customerSpace, entity, version);
        flux = flux.map(cm -> {
            if (cm.getTagList() == null || (cm.getTagList() != null && !cm.getTagList().contains(Tag.EXTERNAL))) {
                cm.setTagList(Collections.singletonList(Tag.INTERNAL));
            }
            return cm;
        });
        if (Boolean.TRUE.equals(allCustomerAttrs)) {
            flux = flux.filter(cm -> // not external (not LDC) or can model
            cm.getTagList().contains(Tag.INTERNAL) || Boolean.TRUE.equals(cm.getCanModel()));
        } else {
            flux = flux.filter(cm -> Boolean.TRUE.equals(cm.getCanModel()));
        }
        return flux;
    }

    private Flux<ColumnMetadata> getSystemMetadataAttrFlux(String customerSpace, BusinessEntity entity,
            DataCollection.Version version) {
        AtomicLong timer = new AtomicLong();
        AtomicLong counter = new AtomicLong();
        Flux<ColumnMetadata> flux;
        flux = servingStoreService.getSystemMetadata(entity,
                version != null ? version : dataCollectionService.getActiveVersion(customerSpace)).sequential();
        flux = flux //
                .doOnSubscribe(s -> {
                    timer.set(System.currentTimeMillis());
                    log.info("Start serving system metadata for " + customerSpace + ":" + customerSpace);
                }) //
                .doOnNext(cm -> counter.getAndIncrement()) //
                .doOnComplete(() -> {
                    long duration = System.currentTimeMillis() - timer.get();
                    log.info("Finished serving system metadata for " + counter.get() + " attributes from "
                            + customerSpace + " TimeElapsed=" + duration + " msec");
                });
        return flux;
    }

}
