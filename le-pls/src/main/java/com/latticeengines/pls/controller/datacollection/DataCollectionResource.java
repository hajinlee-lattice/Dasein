package com.latticeengines.pls.controller.datacollection;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.DataCollectionPrechecks;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.pls.service.DataCollectionPrecheckService;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacollection", description = "REST resource for default metadata data collection")
@RestController
@RequestMapping("/datacollection")
public class DataCollectionResource {
    public static final String ATTR_CONFIG_PATH = "/attrconfig";

    private final DataCollectionProxy dataCollectionProxy;
    private final CDLAttrConfigProxy cdlAttrConfigProxy;
    private final DataCollectionPrecheckService dataCollectionPrecheckService;

    @Inject
    public DataCollectionResource(DataCollectionProxy dataCollectionProxy, CDLAttrConfigProxy cdlAttrConfigProxy,
                                  DataCollectionPrecheckService dataCollectionPrecheckService) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.cdlAttrConfigProxy = cdlAttrConfigProxy;
        this.dataCollectionPrecheckService = dataCollectionPrecheckService;
    }

    @GetMapping(ATTR_CONFIG_PATH)
    @ResponseBody
    @ApiOperation(value = "Get attr config request")
    public AttrConfigRequest getAttrConfigRequest(HttpServletRequest request,
            @RequestParam(value = "entity", required = false) BusinessEntity entity) {
        Tenant tenant = MultiTenantContext.getTenant();
        return cdlAttrConfigProxy.getAttrConfigByEntity(tenant.getId(), entity, true);
    }

    @PostMapping(ATTR_CONFIG_PATH)
    @ResponseBody
    @ApiOperation(value = "Save attr config request")
    public AttrConfigRequest saveAttrConfigRequest(HttpServletRequest request,
            @RequestBody AttrConfigRequest config,
            @RequestParam(value = "mode", required = true) AttrConfigUpdateMode mode) {
        Tenant tenant = MultiTenantContext.getTenant();
        return cdlAttrConfigProxy.saveAttrConfig(tenant.getId(), config, mode);
    }

    @PostMapping(ATTR_CONFIG_PATH + "/validate")
    @ResponseBody
    @ApiOperation(value = "Validate attr config request")
    public AttrConfigRequest validateAttrConfigRequest(HttpServletRequest request,
            @RequestBody AttrConfigRequest config,
            @RequestParam(value = "mode", required = true) AttrConfigUpdateMode mode) {
        Tenant tenant = MultiTenantContext.getTenant();
        return cdlAttrConfigProxy.validateAttrConfig(tenant.getId(), config, mode);
    }

    @GetMapping("/status")
    @ResponseBody
    @ApiOperation(value = "Get attr data collection status")
    public DataCollectionStatus getCollectionStatus(
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
       return dataCollectionProxy.getOrCreateDataCollectionStatus(
               MultiTenantContext.getCustomerSpace().toString(), version);
    }

    @GetMapping("/precheck")
    @ApiOperation(value = "Check whether Account, Product, Transaction and their attributes exist in serving store.")
    public DataCollectionPrechecks precheck() {
        return dataCollectionPrecheckService.validateDataCollectionPrechecks(
                MultiTenantContext.getCustomerSpace().toString());
    }
}
