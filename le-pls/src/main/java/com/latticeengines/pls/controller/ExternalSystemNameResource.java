package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemNameProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "external system name")
@RestController
@RequestMapping("/externalsystemname")
public class ExternalSystemNameResource {

    @Inject
    private CDLExternalSystemNameProxy cdlExternalSystemNameProxy;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get all CDL external system names")
    public Map<BusinessEntity, List<CDLExternalSystemName>> getExternalSystem(HttpServletRequest request) {
        return cdlExternalSystemNameProxy.getExternalSystemNames();
    }

    @GetMapping("/liveramp")
    @ResponseBody
    @ApiOperation(value = "Get all LiveRamp external system names")
    public List<CDLExternalSystemName> getExternalSystemNamesForLiveRamp(HttpServletRequest request) {
        return cdlExternalSystemNameProxy.getExternalSystemNamesForLiveRamp();
    }

}
