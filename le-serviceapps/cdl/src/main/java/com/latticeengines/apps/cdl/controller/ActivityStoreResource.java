package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.service.ActivityStoreService;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.CreateCatalogRequest;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "activities")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/activities")
public class ActivityStoreResource {

    @Inject
    private ActivityStoreService activityStoreService;

    @PostMapping("/catalogs")
    @ResponseBody
    @ApiOperation("Create a catalog under current tenant")
    public Catalog createCatalog(@PathVariable(value = "customerSpace") String customerSpace,
            @RequestBody CreateCatalogRequest request) {
        Preconditions.checkArgument(request != null && StringUtils.isNotBlank(request.getCatalogName()),
                "Request should contains non-blank catalog name");
        return activityStoreService.createCatalog(customerSpace, request.getCatalogName(),
                request.getDataFeedTaskUniqueId());
    }

    @RequestMapping(value = "/catalogs/{catalogName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation("Find catalog by name")
    public Catalog findCatalogByName( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "catalogName") String catalogName) {
        return activityStoreService.findCatalogByTenantAndName(customerSpace, catalogName);
    }

    @PostMapping("/streams")
    @ResponseBody
    @ApiOperation("Create a stream under current tenant")
    public AtlasStream createStream( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @RequestBody AtlasStream stream) {
        return activityStoreService.createStream(customerSpace, stream);
    }

    @RequestMapping(value = "/streams/{streamName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation("Find stream by name")
    public AtlasStream findStreamByName( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "streamName") String streamName, //
            @RequestParam(value = "inflateDimensions", required = false) boolean inflateDimensions) {
        return activityStoreService.findStreamByTenantAndName(customerSpace, streamName, inflateDimensions);
    }

    @RequestMapping(value = "/streams/{streamName}/dimensions/{dimensionName}", method = RequestMethod.PUT)
    @ResponseBody
    @ApiOperation("Update stream dimension dimension")
    public StreamDimension update( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "streamName") String streamName, //
            @PathVariable(value = "dimensionName") String dimensionName, //
            @RequestBody StreamDimension dimension) {
        Preconditions.checkArgument(dimension.getName().equals(dimensionName),
                "Dimension name should match the one in update dimension object");
        return activityStoreService.updateStreamDimension(customerSpace, streamName, dimension);
    }
}
