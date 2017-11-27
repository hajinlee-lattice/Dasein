package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdlexternalsystem", description = "Controller of cdl external system.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/cdlexternalsystem")
public class CDLExternalSystemResource {

    private static final Logger log = LoggerFactory.getLogger(CDLExternalSystemResource.class);

    private final CDLExternalSystemService cdlExternalSystemService;

    @Inject
    public CDLExternalSystemResource(CDLExternalSystemService cdlExternalSystemService) {
        this.cdlExternalSystemService = cdlExternalSystemService;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all CDL external system for a tenant")
    public List<CDLExternalSystem> getExternalSystemList(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return cdlExternalSystemService.getAllExternalSystem(customerSpace);
    }

    @RequestMapping(value = "/{systemType}/accountinterface/{accountInterface}", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a CDL external system for a tenant")
    public void createCDLExternalSystem(@PathVariable String customerSpace, @PathVariable String systemType,
                                        @PathVariable InterfaceName accountInterface) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        cdlExternalSystemService.createExternalSystem(customerSpace, systemType, accountInterface);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a CDL external system for a tenant")
    public void createCDLExternalSystem(@PathVariable String customerSpace,
                    @RequestParam(value = "crmAccount", required = false) InterfaceName crmAccountInt,
                    @RequestParam(value = "mapAccount", required = false) InterfaceName mapAccountInt,
                    @RequestParam(value = "erpAccount", required = false) InterfaceName erpAccountInt) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        cdlExternalSystemService.createExternalSystem(customerSpace, crmAccountInt, mapAccountInt, erpAccountInt);
    }
}
