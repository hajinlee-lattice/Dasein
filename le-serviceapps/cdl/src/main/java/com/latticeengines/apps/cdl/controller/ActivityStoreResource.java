package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CatalogService;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "activities")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/activities")
public class ActivityStoreResource {

    @Inject
    private CatalogService catalogService;

    @PostMapping("/catalogs")
    @ResponseBody
    @ApiOperation("Create a catalog under current tenant")
    public Catalog createCatalog(@PathVariable(value = "customerSpace") String customerSpace,
            @RequestBody Catalog catalog) {
        return catalogService.create(customerSpace, catalog);
    }

    @RequestMapping(value = "/catalogs/{catalogName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation("Find catalog by name")
    public Catalog findCatalogByName( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "catalogName") String catalogName) {
        return catalogService.findByTenantAndName(customerSpace, catalogName);
    }
}
