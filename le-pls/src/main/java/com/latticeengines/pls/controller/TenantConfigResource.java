package com.latticeengines.pls.controller;

import java.util.AbstractMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.pls.service.TenantConfigService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "Tenant config", description = "REST resource for tenant config")
@RestController
@RequestMapping(value = "/config")
@PreAuthorize("hasRole('View_PLS_Configuration')")
public class TenantConfigResource {

    @Autowired
    private TenantConfigService configService;

    @RequestMapping(value = "/topology", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant's topology")
    public TopologyJson getTopology(@RequestParam(value = "tenantId") String tenantId) {
        CRMTopology topology = configService.getTopology(tenantId);
        return new TopologyJson(topology);
    }

    // this class can bubble up the schema to swagger UI
    private class TopologyJson {
        @JsonProperty("Topology")
        public CRMTopology topology;
        public TopologyJson(CRMTopology topology) { this.topology = topology; }
    }
}
