package com.latticeengines.apps.cdl.controller;


import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
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

    @GetMapping(value = "/decoratedmetadata")
    @ResponseBody
    @ApiOperation(value = "Get decorated serving store metadata")
    public Flux<ColumnMetadata> reset(@PathVariable String customerSpace,
                                      @PathVariable BusinessEntity entity) {
        AtomicLong timer = new AtomicLong();
        return servingStoreService.getFullyDecoratedMetadata(entity).sequential()
                .doOnSubscribe(s -> {
                    timer.set(System.currentTimeMillis());
                    log.info("Start serving decorated metadata for " + customerSpace);
                })
                .doOnComplete(() -> {
                    long duration = System.currentTimeMillis() - timer.get();
                    log.info("Finished serving decorated metadata for " + customerSpace + " TimeElapsed=" + duration + " msec");
                });
    }
}
