package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

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

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get all CDL external system for a tenant")
    public CDLExternalSystem getExternalSystem(@PathVariable String customerSpace,
            @RequestParam(value = "entity", required = false, defaultValue = "Account") String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return cdlExternalSystemService.getExternalSystem(customerSpace, BusinessEntity.getByName(entity));
    }

    @GetMapping("/map")
    @ResponseBody
    @ApiOperation(value = "Get all CDL external system for a tenant")
    public Map<String, List<CDLExternalSystemMapping>> getExternalSystemMap(@PathVariable String customerSpace,
            @RequestParam(value = "entity", required = false, defaultValue = "Account") String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return cdlExternalSystemService.getExternalSystemMap(customerSpace, BusinessEntity.getByName(entity));
    }

    @GetMapping("/type/{type}")
    @ResponseBody
    @ApiOperation(value = "Get all CDL external system for a tenant")
    public List<CDLExternalSystemMapping> getExternalSystemByType(@PathVariable String customerSpace,
            @PathVariable CDLExternalSystemType type,
            @RequestParam(value = "entity", required = false, defaultValue = "Account") String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return cdlExternalSystemService.getExternalSystemByType(customerSpace, type, BusinessEntity.getByName(entity));
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create or Update a CDL external system for a tenant")
    public void createOrUpdateCDLExternalSystem(@PathVariable String customerSpace,
            @RequestBody CDLExternalSystem cdlExternalSystem) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        cdlExternalSystemService.createOrUpdateExternalSystem(customerSpace, cdlExternalSystem,
                cdlExternalSystem.getEntity());
    }
}
