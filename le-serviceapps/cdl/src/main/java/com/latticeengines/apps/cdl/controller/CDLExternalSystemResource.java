package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;

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
    public CDLExternalSystem getExternalSystem(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return cdlExternalSystemService.getExternalSystem(customerSpace);
    }

    @RequestMapping(value = "/map", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all CDL external system for a tenant")
    public Map<String, List<CDLExternalSystemMapping>> getExternalSystemMap(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return cdlExternalSystemService.getExternalSystemMap(customerSpace);
    }

    @RequestMapping(value = "/type/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all CDL external system for a tenant")
    public List<CDLExternalSystemMapping> getExternalSystemByType(@PathVariable String customerSpace,
                                                                  @PathVariable CDLExternalSystemType type) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return cdlExternalSystemService.getExternalSystemByType(customerSpace, type);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or Update a CDL external system for a tenant")
    public void createOrUpdateCDLExternalSystem(@PathVariable String customerSpace,
                                                @RequestBody CDLExternalSystem cdlExternalSystem) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem);
    }
}
