package com.latticeengines.apps.cdl.controller;

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
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import reactor.core.publisher.Flux;

@Api(value = "serving store", description = "REST resource for serving stores")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/servingstore/{entity}")
public class ServingStoreResource {

    private static final Logger log = LoggerFactory.getLogger(ServingStoreResource.class);

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private DataCollectionService dataCollectionService;

    @GetMapping(value = "/decoratedmetadata")
    @ResponseBody
    @ApiOperation(value = "Get decorated serving store metadata")
    public Flux<ColumnMetadata> getDecoratedMetadata( //
            @PathVariable String customerSpace, @PathVariable BusinessEntity entity, //
            @RequestParam(name = "groups", required = false) List<ColumnSelection.Predefined> groups, //
            @RequestParam(name = "version", required = false) DataCollection.Version version
    ) {
        return getFlux(customerSpace, entity, version, groups);
    }

    private Flux<ColumnMetadata> getFlux(String customerSpace, BusinessEntity entity, DataCollection.Version version,
            List<ColumnSelection.Predefined> groups) {
        AtomicLong timer = new AtomicLong();
        AtomicLong counter = new AtomicLong();
        Flux<ColumnMetadata> flux;
        if (version == null) {
            flux = servingStoreService.getFullyDecoratedMetadata(entity,
                    dataCollectionService.getActiveVersion(customerSpace)).sequential();
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

}
