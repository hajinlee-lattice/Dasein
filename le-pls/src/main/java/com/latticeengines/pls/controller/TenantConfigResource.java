package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.pls.service.TenantConfigService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Tenant config", description = "REST resource for tenant config")
@RestController
@RequestMapping(value = "/config")
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

    @RequestMapping(value = "/featureflags", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant's feature flags")
    public FeatureFlagValueMap getFeatureFlags(@RequestParam(value = "tenantId") String tenantId) {
        return configService.getFeatureFlags(tenantId);
    }

    @RequestMapping(value = "/dataloaderurl", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant's DataLoader url")
    public String getDataLoaderUrl(@RequestParam(value = "tenantId") String tenantId) throws Exception {
        String url = configService.getDLRestServiceAddress(tenantId);
        url = configService.removeDLRestServicePart(url);
        return JsonUtils.serialize(url);
    }

    // this class can bubble up the schema to swagger UI
    private class TopologyJson {
        @JsonProperty("Topology")
        public CRMTopology topology;
        public TopologyJson(CRMTopology topology) { this.topology = topology; }
    }
}
