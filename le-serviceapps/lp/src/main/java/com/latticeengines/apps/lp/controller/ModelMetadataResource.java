package com.latticeengines.apps.lp.controller;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.ModelMetadataService;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.serviceapps.lp.ModelFieldsToAttributesRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "model metadata", description = "REST resource for model metadata")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/modelmetadata")
public class ModelMetadataResource {

    @Inject
    private ModelMetadataService modelMetadataService;

    @GetMapping("/modelid/{modelId}/metadata")
    @ResponseBody
    @ApiOperation(value = "Get model metadata")
    public List<VdbMetadataField> getMetadata(@PathVariable String customerSpace, @PathVariable String modelId) {
        return modelMetadataService.getMetadata(modelId);
    }

    @GetMapping("/modelid/{modelId}/training-table")
    @ResponseBody
    @ApiOperation(value = "Get training table")
    public Table getTrainingTableFromModelId(@PathVariable String customerSpace, @PathVariable String modelId) {
        return modelMetadataService.getTrainingTableFromModelId(modelId);
    }

    @GetMapping("/modelid/{modelId}/event-table")
    @ResponseBody
    @ApiOperation(value = "Get event table")
    public Table getEventTableFromModelId(@PathVariable String customerSpace, @PathVariable String modelId) {
        return modelMetadataService.getEventTableFromModelId(modelId);
    }

    @GetMapping("/modelid/{modelId}/required-column-names")
    @ResponseBody
    @ApiOperation(value = "Get required column display names")
    public List<String> getRequiredColumnDisplayNames(@PathVariable String customerSpace,
            @PathVariable String modelId) {
        return modelMetadataService.getRequiredColumnDisplayNames(modelId);
    }

    @GetMapping("/modelid/{modelId}/required-columns")
    @ResponseBody
    @ApiOperation(value = "Get required columns")
    public List<Attribute> getRequiredColumns(@PathVariable String customerSpace, @PathVariable String modelId) {
        return modelMetadataService.getRequiredColumns(modelId);
    }

    @GetMapping("/modelid/{modelId}/lattice-attr-names")
    @ResponseBody
    @ApiOperation(value = "Get lattice attribute names")
    public Set<String> getLatticeAttributeNames(@PathVariable String customerSpace, @PathVariable String modelId) {
        return modelMetadataService.getLatticeAttributeNames(modelId);
    }

    @GetMapping("/attributes-from-fields")
    @ResponseBody
    @ApiOperation(value = "Get customized attributes from fields")
    public List<Attribute> getAttributesFromFields(@PathVariable String customerSpace,
                                                   @RequestBody ModelFieldsToAttributesRequest request) {
        return modelMetadataService.getAttributesFromFields(request.getAttributes(), request.getFields());
    }

}
