package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.service.ActivityStoreService;
import com.latticeengines.apps.cdl.service.DimensionMetadataService;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.CreateCatalogRequest;
import com.latticeengines.domain.exposed.cdl.activity.DeriveConfig;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.KeysWrapper;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "activities")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/activities")
public class ActivityStoreResource {

    @Inject
    private ActivityStoreService activityStoreService;

    @Inject
    @Lazy
    private DimensionMetadataService dimensionMetadataService;

    @PostMapping("/catalogs")
    @ResponseBody
    @ApiOperation("Create a catalog under current tenant")
    public Catalog createCatalog(@PathVariable(value = "customerSpace") String customerSpace,
            @RequestBody CreateCatalogRequest request) {
        Preconditions.checkArgument(request != null && StringUtils.isNotBlank(request.getCatalogName()),
                "Request should contains non-blank catalog name");
        return activityStoreService.createCatalog(customerSpace, request.getCatalogName(),
                request.getDataFeedTaskUniqueId(), request.getPrimaryKeyColumn());
    }

    @GetMapping("/catalogs/{catalogName}")
    @ResponseBody
    @ApiOperation("Find catalog by name")
    public Catalog findCatalogByName( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "catalogName") String catalogName) {
        return activityStoreService.findCatalogByTenantAndName(customerSpace, catalogName);
    }

    @GetMapping("/streams")
    @ResponseBody
    @ApiOperation("Get all streams under current tenant")
    public List<AtlasStream> getStreams(@PathVariable(value = "customerSpace") String customerSpace) {
        return activityStoreService.getStreams(customerSpace);
    }

    @PostMapping("/streams")
    @ResponseBody
    @ApiOperation("Create a stream under current tenant")
    public AtlasStream createStream( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @RequestBody AtlasStream stream) {
        return activityStoreService.createStream(customerSpace, stream);
    }

    @GetMapping("/streams/{streamName}")
    @ResponseBody
    @ApiOperation("Find stream by name")
    public AtlasStream findStreamByName( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "streamName") String streamName, //
            @RequestParam(value = "inflateDimensions", required = false) boolean inflateDimensions) {
        return activityStoreService.findStreamByTenantAndName(customerSpace, streamName, inflateDimensions);
    }

    @PutMapping("/streams/{streamName}/dimensions/{dimensionName}")
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

    @PostMapping("/streams/{streamName}/dimensions/add/derivedDimension")
    @ResponseBody
    @ApiOperation("add a derived dimension to stream")
    public boolean addDerivedDimension( //
                                   @PathVariable(value = "customerSpace") String customerSpace, //
                                   @PathVariable(value = "streamName") String streamName, //
                                   @RequestBody DeriveConfig deriveConfig) {
        return activityStoreService.addDeriveDimensionConfig(customerSpace, streamName, deriveConfig);
    }

    @PostMapping("/dimensionMetadata")
    @ResponseBody
    @ApiOperation("Save dimension metadata with generated signature")
    public String saveDimensionMetadata( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @RequestBody Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        return activityStoreService.saveDimensionMetadata(customerSpace, null, dimensionMetadataMap);
    }

    @PostMapping("/dimensionMetadata/{signature}")
    @ResponseBody
    @ApiOperation("Save dimension metadata with given signature, return final signature after combining with tenant namespace")
    public String saveDimensionMetadataWithSignature( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "signature") String signature, //
            @RequestBody Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap) {
        return activityStoreService.saveDimensionMetadata(customerSpace, signature, dimensionMetadataMap);
    }

    @GetMapping("/dimensionMetadata")
    @ResponseBody
    @ApiOperation("Retrieve all dimension metadata with target signature and tenant")
    public Map<String, Map<String, DimensionMetadata>> getDimensionMetadata( //
            @PathVariable(value = "customerSpace") String customerSpace,
            @RequestParam(value = "signature", required = false) String signature,
            @RequestParam(value = "withStreamName", defaultValue = "true", required = false) boolean withStreamName) {
        return activityStoreService.getDimensionMetadata(customerSpace, signature, withStreamName);
    }

    @GetMapping("/dimensionMetadata/streams/{streamName}")
    @ResponseBody
    @ApiOperation("Retrieve dimension metadata of given stream with target signature and tenant")
    public Map<String, DimensionMetadata> getDimensionMetadataInStream( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "streamName") String streamName, //
            @RequestParam(value = "signature", required = false) String signature) {
        return activityStoreService.getDimensionMetadataInStream(customerSpace, streamName, signature);
    }

    @PostMapping("/dimensionIds")
    @ResponseBody
    @ApiOperation("Allocate dimension IDs for given dimension values")
    public Map<String, String> allocateDimensionIds( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @RequestBody KeysWrapper dimensionValues) {
        return activityStoreService.allocateDimensionId(customerSpace, dimensionValues.getKeys());
    }

    /*-
     * use POST for id to value & value to id lookup to prevent url from being too long
     */

    @PostMapping("/dimensionIdsByValues")
    @ResponseBody
    @ApiOperation("Lookup allocated dimension IDs by given dimension values")
    public Map<String, String> getDimensionIds( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @RequestBody KeysWrapper dimensionValues) {
        return activityStoreService.getDimensionIds(customerSpace, dimensionValues.getKeys());
    }

    @PostMapping("/dimensionValuesByIds")
    @ResponseBody
    @ApiOperation("Use allocated dimension IDs to lookup dimension values")
    public Map<String, String> getDimensionValues( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @RequestBody KeysWrapper dimensionIds) {
        return activityStoreService.getDimensionValues(customerSpace, dimensionIds.getKeys());
    }

    @GetMapping("/metricsGroups/groupId/{groupId}")
    @ResponseBody
    @ApiOperation("Retrieve metricsGroup by tenant and groupId")
    public ActivityMetricsGroup findGroupByGroupId(@PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "groupId") String groupId) {
        return activityStoreService.findGroupByGroupId(customerSpace, groupId);
    }

    @GetMapping("/metricsGroups")
    @ResponseBody
    @ApiOperation("Retrieve metricsGroup list by tenant")
    public List<ActivityMetricsGroup> findGroupsByTenant(@PathVariable(value = "customerSpace") String customerSpace) {
        return activityStoreService.findByTenant(customerSpace);
    }

    @DeleteMapping("/dimensionMetadata/{signature}")
    @ResponseBody
    @ApiOperation("Clear all dimension metadata associated with given signature")
    public void deleteDimensionMetadataWithSignature( //
            @PathVariable(value = "customerSpace") String customerSpace, //
            @PathVariable(value = "signature") String signature) {
        // TODO make sure signature belongs to tenant
        dimensionMetadataService.delete(signature);
    }
}
