package com.latticeengines.metadata.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.retention.RetentionPolicy;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.service.impl.TableResourceHelper;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "importtable", description = "REST resource for import table definition")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class ImportTableResource {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ImportTableResource.class);

    @Inject
    private TableResourceHelper tableResourceHelper;

    @GetMapping("/importtables")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public List<String> getTables(@PathVariable String customerSpace) {
        return tableResourceHelper.getTables(customerSpace);
    }

    @GetMapping("/importtables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Get table by name")
    public Table getTable(@PathVariable String customerSpace, @PathVariable String tableName) {
        return tableResourceHelper.getTable(customerSpace, tableName, true);
    }

    @GetMapping("/importtables/{tableName}/metadata")
    @ResponseBody
    @ApiOperation(value = "Get table metadata by name")
    public ModelingMetadata getTableMetadata(@PathVariable String customerSpace, @PathVariable String tableName) {
        return tableResourceHelper.getTableMetadata(customerSpace, tableName);
    }

    @PostMapping("/importtables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Create table")
    public Boolean createTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table) {
        return tableResourceHelper.createTable(customerSpace, tableName, table);
    }

    @PutMapping("/importtables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Update table")
    public Boolean updateTable(@PathVariable String customerSpace, //
            @PathVariable String tableName, //
            @RequestBody Table table) {
        return tableResourceHelper.updateTable(customerSpace, tableName, table);
    }

    @PostMapping("/importtables/{tableName}/fixattributes")
    @ResponseBody
    @ApiOperation(value = "Fix table attributes")
    public Boolean fixTableAttributes(@PathVariable String customerSpace, @PathVariable String tableName,
                                      @RequestBody List<AttributeFixer> attributeFixerList) {
        return tableResourceHelper.fixTableAttributes(customerSpace, tableName, attributeFixerList);
    }

    @DeleteMapping("/importtables/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Delete table")
    public Boolean deleteTable(@PathVariable String customerSpace, //
            @PathVariable String tableName) {
        return tableResourceHelper.deleteImportTableAndCleanup(customerSpace, tableName);
    }

    @PutMapping("/importtables/{tableName}/policy")
    @ResponseBody
    @ApiOperation(value = "Update import table retention policy")
    public Boolean updateTableRetentionPolicy(@PathVariable String customerSpace, @PathVariable(value = "tableName") String tableName,
                                              @RequestBody RetentionPolicy retentionPolicy) {
        return tableResourceHelper.updateTableRetentionPolicy(customerSpace, tableName, retentionPolicy);
    }

}
