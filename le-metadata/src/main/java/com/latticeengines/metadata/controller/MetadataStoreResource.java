package com.latticeengines.metadata.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.metadata.service.MetadataStoreService;

import io.swagger.annotations.Api;
import reactor.core.publisher.Flux;

@Api(value = "MetadataStore", description = "REST resource for metadata store")
@RestController
@RequestMapping("/metadatastore")
public class MetadataStoreResource {

    @Inject
    private MetadataStoreService metadataStoreService;

    @GetMapping("/{mdsName}/namespace/{namespace}")
    public Flux<ColumnMetadata> getMetadata(//
            @PathVariable(name = "mdsName") String mdsName, //
            @PathVariable(name = "namespace") String[] namespace) {
        return metadataStoreService.getMetadata(mdsName, namespace);
    }

    @GetMapping("/{mdsName}/namespace/{namespace}/count")
    public Long count(//
            @PathVariable(name = "mdsName") String mdsName, //
            @PathVariable(name = "namespace") String[] namespace) {
        return metadataStoreService.count(mdsName, namespace);
    }

}
