package com.latticeengines.matchapi.controller;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.service.AccountMasterStatisticsService;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "match", description = "REST resource for account master statistics")
@RestController
@RequestMapping("/amstats")
public class AMStatsResource {
    private static final Log log = LogFactory.getLog(AMStatsResource.class);

    @Autowired
    private AccountMasterStatisticsService accountMasterStatisticsService;

    @RequestMapping(value = "/cubes", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account master statistics cube")
    private AccountMasterCube getCube(@RequestBody AccountMasterFactQuery query, HttpServletResponse response) {
        return accountMasterStatisticsService.query(query);
    }

    @RequestMapping(value = "/topattrs", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get categorical attribute tree")
    private TopNAttributeTree getTopAttrTree() {
        return accountMasterStatisticsService.getTopAttrTree();
    }
}