package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLExternalSystemNameService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdl external system name")
@RestController
@RequestMapping("/cdlexternalsystemname")
public class CDLExternalSystemNameResource {

    private static final Logger log = LoggerFactory.getLogger(CDLExternalSystemNameResource.class);

    @Inject
    private CDLExternalSystemNameService cdlExternalSystemNameService;

    @GetMapping("")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "Get CDL external system names")
    public Map<BusinessEntity, List<CDLExternalSystemName>> getExternalSystemNames() {
            return cdlExternalSystemNameService.getAllExternalSystemNames();
    }

    @GetMapping("/liveramp")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "Get all LiveRamp external system names")
    public List<CDLExternalSystemName> getExternalSystemNamesForLiveRamp() {
        return cdlExternalSystemNameService.getExternalSystemNamesForLiveRamp();
    }
}
