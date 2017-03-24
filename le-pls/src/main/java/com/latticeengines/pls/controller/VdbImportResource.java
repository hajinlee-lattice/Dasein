package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.VdbGetLoadStatusConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableCancel;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;
import com.latticeengines.pls.service.VdbImportService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "vdbimport", description = "REST resource for table import in VisiDB")
@RestController
@RequestMapping(value = "/vdbimport")
public class VdbImportResource {

    @Autowired
    private VdbImportService vdbImportService;

    @RequestMapping(value = "/loadvisidbdata", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Start load VisiDB table job")
    public String submitLoadTableJob(@RequestBody VdbLoadTableConfig config, HttpServletRequest request) {
        return JsonUtils.serialize(ImmutableMap.of("application_id", vdbImportService.submitLoadTableJob(config)));
    }

    @RequestMapping(value = "/{applicationId}/cancel", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Cancel load table job")
    public String cancelLoadTableJob(@PathVariable String applicationId, @RequestBody VdbLoadTableCancel cancelConfig,
            HttpServletRequest request) {
        return JsonUtils.serialize(ImmutableMap.of("success", vdbImportService.cancelLoadTableJob(applicationId,
                cancelConfig)));
    }

    @RequestMapping(value = "/checkstatus", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Check load VisiDB table job")
    public VdbLoadTableStatus getLoadTableStatus(@RequestBody VdbGetLoadStatusConfig config, HttpServletRequest request) {
        return vdbImportService.getLoadTableStatus(config);
    }
}
