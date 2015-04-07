package com.latticeengines.admin.controller;

import java.util.AbstractMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.admin.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;


@Api(value = "tenantadmin", description = "REST resource for managing Lattice tenants across all products")
@RestController
@RequestMapping(value = "/tenants")
public class TenantResource {

    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @RequestMapping(value = "/{tenantId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Bootstrap a Lattice tenant")
    public void bootstrap(@RequestParam String tenantId) {
       
    }
    
    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenants for a particular contract id")
    public List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(@RequestParam(value = "contractId", required = false) String contractId) {
        if (StringUtils.isEmpty(contractId)) {
            contractId = null;
        }
        return tenantEntityMgr.getTenants(contractId);
    }
}
