package com.latticeengines.matchapi.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.matchapi.service.AccountMasterStatisticsService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "amstats", description = "REST resource for account master statistics")
@RestController
@RequestMapping("/amstats")
public class AMStatsResource {

    @Inject
    private AccountMasterStatisticsService accountMasterStatisticsService;

    @PostMapping("/cubes")
    @ResponseBody
    @ApiOperation(value = "Get account master statistics cube", response = AccountMasterCube.class)
    public AccountMasterCube getCube(@RequestBody AccountMasterFactQuery query,
            @RequestParam(value = "considerOnlyEnrichments", required = false, //
                    defaultValue = "true") boolean considerOnlyEnrichments) {
        return accountMasterStatisticsService.query(query, considerOnlyEnrichments);
    }

    @GetMapping("/topattrs")
    @ResponseBody
    @ApiOperation(value = "Get categorical attribute tree", response = TopNAttributeTree.class)
    public TopNAttributeTree getTopAttrTree() {
        return accountMasterStatisticsService.getTopAttrTree();
    }

}
