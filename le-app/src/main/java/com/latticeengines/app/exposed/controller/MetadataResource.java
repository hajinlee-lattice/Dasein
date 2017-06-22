package com.latticeengines.app.exposed.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

//TODO: not sure if this one is still needed
@Api(value = "metadata", description = "Common REST resource for retrieving attribute-level metadata")
@RestController
@RequestMapping("/metadata")
public class MetadataResource {
    @Autowired
    private MetadataService metadataService;

    @RequestMapping(value = "/attributes/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get number of attributes")
    private int getAttributesCount() {
        return metadataService.getAttributes(null, null).size();
    }

    @RequestMapping(value = "/attributes", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of attributes")
    private List<ColumnMetadata> getAttributes( //
            @ApiParam(value = "Offset for pagination of matching attributes")//
            @RequestParam(value = "offset", required = false)//
            Integer offset, //
            @ApiParam(value = "Maximum number of matching attributes in page")//
            @RequestParam(value = "max", required = false)//
            Integer max) {
        return metadataService.getAttributes(offset, max);
    }

    @RequestMapping(value = "/statistics", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get statistics")
    private Statistics getStatistics() {
        return metadataService.getStatistics();
    }
}
