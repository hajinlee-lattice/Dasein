package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

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

    @GetMapping("/{entity}/decoratedmetadata")
    @ResponseBody
    @ApiOperation(value = "Get decorated serving store metadata")
    public Flux<ColumnMetadata> getDecoratedMetadata(@PathVariable String customerSpace, @PathVariable BusinessEntity entity, //
                                                     @RequestParam(name = "groups", required = false) List<ColumnSelection.Predefined> groups, //
                                                     @RequestParam(name = "attributeSetName", required = false) String attributeSetName,
                                                     @RequestParam(name = "version", required = false) DataCollection.Version version,
                                                     @RequestParam(name = "filter", required = false) StoreFilter filter) {
        return servingStoreService.getDecoratedMetadata(customerSpace, entity, version, groups, attributeSetName, filter);
    }

    @GetMapping("/systemmetadata")
    @ResponseBody
    @ApiOperation(value = "Get system metadata attributes")
    public Flux<ColumnMetadata> getSystemMetadataAttrs( //
            @PathVariable String customerSpace, //
            @RequestParam(name = "entity", required = true) BusinessEntity entity, //
            @RequestParam(name = "version", required = false) DataCollection.Version version) {
        return servingStoreService.getSystemMetadataAttrFlux(customerSpace, entity, version);
    }

    @GetMapping("/new-modeling")
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
        return servingStoreService.getAttrsEnabledForModeling(customerSpace, entity, version);
    }

    @GetMapping("/allow-modeling")
    @ResponseBody
    @ApiOperation(value = "Get attributes that are allowed for modeling")
    public Flux<ColumnMetadata> getAllowedModelingAttrs( //
            @PathVariable String customerSpace, //
            @RequestParam(name = "entity", required = false, defaultValue = "Account") BusinessEntity entity, //
            @RequestParam(name = "version", required = false) DataCollection.Version version, //
            @RequestParam(name = "all-customer-attrs", required = false) Boolean allCustomerAttrs) {
        log.info(String.format("Get allow modeling attributes for %s with entity %s", customerSpace, entity));
        return servingStoreService.getAttrsCanBeEnabledForModeling(customerSpace, entity, version, allCustomerAttrs);
    }

    @PostMapping("/{entity}/attrs-usage")
    @ResponseBody
    @ApiOperation(value = "Get attributes usage")
    public Map<String, Boolean> getAttrsUsage( //
            @PathVariable String customerSpace, //
            @PathVariable BusinessEntity entity, //
            @RequestParam(name = "groups", required = true) ColumnSelection.Predefined group, //
            @RequestParam(name = "version", required = false) DataCollection.Version version, //
            @RequestBody Set<String> attrs) {
        return servingStoreService.getAttributesUsage(customerSpace, entity, attrs, group, version);
    }

}
