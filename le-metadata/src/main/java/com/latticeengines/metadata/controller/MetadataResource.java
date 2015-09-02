package com.latticeengines.metadata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.service.MetadataService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata")
@RestController
@RequestMapping("/tenants/{tenantId}")
public class MetadataResource {
    
    @Autowired
    private MetadataService mdService;

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public Table getTable(@PathVariable String tenantId, @PathVariable String tableName) {
        return null;
    }

    @RequestMapping(value = "/tables/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create table")
    public void createTable(@RequestBody Table table) {
    }

    @RequestMapping(value = "tables/{tableName}/extracts", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add extract to existing table")
    public void addExtractToTable(@RequestBody Extract extract) {
    }
}
