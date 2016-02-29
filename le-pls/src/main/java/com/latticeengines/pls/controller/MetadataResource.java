package com.latticeengines.pls.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for interacting with Table metadata")
@RestController
@RequestMapping("/metadata")
@PreAuthorize("hasRole('Edit_PLS_Data')")
public class MetadataResource {
    private static final Logger log = Logger.getLogger(MetadataResource.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @RequestMapping(value = "/{tableName}", method = RequestMethod.PUT)
    @ResponseBody
    @ApiOperation(value = "Update the metadata for the specified table")
    public void updateTable(@PathVariable String tableName, @RequestBody Table table) {
        String customer = SecurityContextUtils.getCustomerSpace().toString();
        metadataProxy.updateTable(customer, tableName, table);
    }

    @RequestMapping(value = "/{tableName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Retrieve the metadata for the specified table")
    public Table getTable(@PathVariable String tableName) {
        String customer = SecurityContextUtils.getCustomerSpace().toString();
        return metadataProxy.getTable(customer, tableName);
    }
}
