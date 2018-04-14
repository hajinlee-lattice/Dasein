package com.latticeengines.pls.controller.datacollection;


import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacollection", description = "REST resource for default metadata data collection")
@RestController
@RequestMapping("/datacollection")
public class DataCollectionResource {

    public static final String ATTR_CONFIG_PATH = "/attrconfig";

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @RequestMapping(value = ATTR_CONFIG_PATH, //
            method = RequestMethod.GET, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get attr config request")
    public AttrConfigRequest getAttrConfigRequest(HttpServletRequest request,
            @RequestParam(value = "entity", required = false) BusinessEntity entity) {
        Tenant tenant = MultiTenantContext.getTenant();
        return cdlAttrConfigProxy.getAttrConfigByEntity(tenant.getId(), entity, true);
    }

    @RequestMapping(value = ATTR_CONFIG_PATH, //
            method = RequestMethod.POST, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Save attr config request")
    public AttrConfigRequest saveAttrConfigRequest(HttpServletRequest request,
            @RequestBody AttrConfigRequest config) {
        Tenant tenant = MultiTenantContext.getTenant();
        return cdlAttrConfigProxy.saveAttrConfig(tenant.getId(), config);
    }

    @RequestMapping(value = ATTR_CONFIG_PATH + "/validate", //
            method = RequestMethod.POST, //
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validate attr config request")
    public AttrConfigRequest validateAttrConfigRequest(HttpServletRequest request,
            @RequestBody AttrConfigRequest config) {
        Tenant tenant = MultiTenantContext.getTenant();
        return cdlAttrConfigProxy.validateAttrConfig(tenant.getId(), config);
    }

}
