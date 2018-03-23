package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.impl.CDLAttrConfigServiceImpl;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "AttrConfig", description = "REST resource for default LP attribute config.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/attrconfig")
public class CDLAttrConfigResource {
    @Inject
    private CDLAttrConfigServiceImpl cdlAttrConfigSevice;

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation("get cdl attribute config request")
    public AttrConfigRequest getAttrConfig(@PathVariable String customerSpace,
            @RequestParam(value = "entity", required = false) BusinessEntity entity) {
        AttrConfigRequest request = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = cdlAttrConfigSevice.getRenderedList(customerSpace, entity);
        request.setAttrConfigs(attrConfigs);
        return request;
    }
}
