package com.latticeengines.apps.lp.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.impl.LPAttrConfigServiceImpl;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "AttrConfig", description = "REST resource for default LP attribute config.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/attrconfig")
public class LPAttrConfigResource {

    @Inject
    private LPAttrConfigServiceImpl lpAttrConfigSevice;

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation("get attribute config request")
    public AttrConfigRequest getAttrConfig(@PathVariable String customerSpace) {
        AttrConfigRequest request = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = lpAttrConfigSevice.getRenderedList(customerSpace, BusinessEntity.LatticeAccount);
        request.setAttrConfigs(attrConfigs);
        return request;
    }
}
