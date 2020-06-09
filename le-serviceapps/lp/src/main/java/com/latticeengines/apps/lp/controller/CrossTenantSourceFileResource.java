package com.latticeengines.apps.lp.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.domain.exposed.pls.SourceFile;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cross-tenant sourcefile", description = "REST resource for cross-tenant sourcefile management")
@RestController
@RequestMapping("/sourcefiles")
public class CrossTenantSourceFileResource {

    @Inject
    private SourceFileService sourceFileService;

    @GetMapping("/tablename/{tableName}")
    @ResponseBody
    @ApiOperation(value = "Find source file by table name cross tenants")
    @NoCustomerSpace
    public SourceFile findByTableName(@PathVariable String tableName) {
        return sourceFileService.getByTableNameCrossTenant(tableName);
    }

}
